import logging
from typing import List
from typing import Optional
from typing import Tuple
from typing import Iterator
from concurrent.futures import ThreadPoolExecutor, wait, as_completed
from multiprocessing import current_process
from itertools import product
import warnings

import numpy
import cupy
import scipy.ndimage.measurements
from preadator import ProcessManager

import utils

logging.basicConfig(
    format="%(asctime)s - %(name)-8s - %(levelname)-8s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
)
logger = logging.getLogger("dynamics")
logger.setLevel(utils.POLUS_LOG)


def geometric_median(mask: numpy.ndarray) -> numpy.ndarray:
    """Calculate the geometric median for a mask

    The geometric median is the data point closest to the median, ensuring that the
    median value coincides with an actual data point. This is accomplished by finding
    the point with the smallest euclidian distance from the standard median.

    Args:
        mask (numpy.ndarray): A binary mask

    Returns:
        numpy.ndarray: The coordinates of the geometric median.
    """

    # Get the geometric median of the object
    non_zero_indices = numpy.argwhere(mask)
    median = numpy.median(non_zero_indices, axis=0)
    geo_coords = numpy.argmin(numpy.sum((non_zero_indices - median) ** 2, axis=1))

    return non_zero_indices[geo_coords]


def stack_rois(
    image: numpy.ndarray, slices: List[Tuple[slice, slice]], device: str
) -> Iterator[Tuple[numpy.ndarray, numpy.ndarray, numpy.ndarray]]:
    """Stack multiple ROIs into a single n-d array

    This function stacks all ROIs along the 0th dimension such that the centroid of each
    object occurs at the same higher dimensional coordinates. For example, in the case
    of 2-dimensional images, the `image` is the original image and `slices` is a list
    of slices indicating which objects to stack. If there are N objects, then the
    centroid will occur at [:,Y,X], where [Y,X] is the centroid for every object where
    [0,:,:] is the first object, [1,:,:] is the second object, etc.

    To help conserve memory, this function is a generator. Each iteration will yield
    roughly the TILE_SIZE**2 number of pixels.

    Args:
        image (numpy.ndarray): A labeled image
        slices (List[Tuple[slice, slice]]): A list of slices, one for each ROI.
        device (str): Must be either "cpu" or "gpu"

    Yields:
        Iterator[Tuple[numpy.ndarray, numpy.ndarray]]: [description]
    """

    xp = cupy if device == "gpu" else numpy

    if device == "gpu":
        image = xp.asarray(image)

    # Set the maximum number of pixels to process at a time
    max_pixels = utils.TILE_SIZE ** 2
    current_label = 0
    dims = image.ndim

    # Outermost loop, resets values for next generation
    while current_label < len(slices):

        masks = []
        extents = []
        total_pixels = 0  # Total number of pixels in current generation
        max_slice_pixels = 0  # width*height of the largest ROI in the generation

        # Stack a set of ROIs for the current generation
        while total_pixels < max_pixels and current_label < len(slices):

            # Create the mask and append to the list of masks
            mask = image[slices[current_label]] == (current_label + 1)

            # Check if the current ROI is larger than the largest ROI
            current_size = mask.size
            if current_size > max_slice_pixels:

                # If the current ROI would give too many pixels, load it next iteration
                if current_size * len(masks) > max_pixels:
                    break

                else:
                    max_slice_pixels = mask.size

            # Get the geometric median of the object
            geo_median = geometric_median(mask)

            # Calculate the extents of the region relative to the median
            extent = xp.zeros((2 * dims,))
            extent[:dims] = -geo_median
            extent[dims:] = xp.asarray(mask.shape) - geo_median

            masks.append(mask)
            extents.append(extent)

            current_label += 1
            total_pixels = max_slice_pixels * len(masks)

        # Calculate the size of output needed to store all ROIs with the same centroid
        extents = xp.asarray(extents).astype(int)
        centroid = -xp.min(extents[:, :dims], axis=0) + 2
        shape = xp.max(extents[:, dims:], axis=0) + centroid + 1

        # Calculate offsets for each ROI to position the centroid at the same location
        extents[:, :dims] += centroid
        extents[:, dims:] += centroid

        # Create the output, and stack the ROIs
        stack = xp.zeros([len(masks)] + shape.astype(int).tolist(), dtype=xp.bool_)

        sub_slices = []

        for i, mask in enumerate(masks):

            ext = extents[i]

            slice_tuple = tuple(slice(ext[i], ext[i + dims]) for i in range(dims))

            sub_slices.append(slice_tuple)

            stack[(i,) + slice_tuple] = mask

        yield stack, sub_slices, centroid


def box_filt_2d(image, w=3):

    integral_image = image.cumsum(axis=1).cumsum(axis=2)
    mean_filt = (
        integral_image[:, w:, w:]
        - integral_image[:, w:, :-w]
        - integral_image[:, :-w, w:]
        + integral_image[:, :-w, :-w]
    )

    return mean_filt


def box_filt_3d(image, w=3):

    integral_image = image.cumsum(axis=1).cumsum(axis=2).cumsum(axis=3)
    mean_filt = (
        integral_image[:, w:, w:, w:]
        + integral_image[:, w:, :-w, :-w]
        + integral_image[:, :-w, w:, :-w]
        + integral_image[:, :-w, :-w, w:]
        - integral_image[:, :-w, :-w, :-w]
        - integral_image[:, :-w, w:, w:]
        - integral_image[:, w:, :-w, w:]
        - integral_image[:, w:, w:, :-w]
    )

    return mean_filt


def creeping_mean_filter(
    stack: numpy.ndarray, centroid: numpy.ndarray, label_count: int, device: str
) -> numpy.ndarray:
    """Integral image based diffusion

    This function uses integral images to simulate diffusion using mean filters. The
    creeping component has to do with how the integral images are generated on each
    iteration. A flame is applied to the centroid location on each iteration. On the
    first iteration, it is only possible for pixels in the 3x3 region centered on the
    flame to have heat diffusion occur. Therefore, the first iteration only applies
    the integral image to the first 3x3 set of pixels around the centroid. The next
    iteration "creeps" the boundary outward by one pixel in all directions, so that the
    integral image is only applied to a 5x5 set of pixels centered on the flame. The
    creeping boundaries stop expanding once they hit the outer limits of the ROI stack.

    Every 10 iterations, a check is run to determine which ROIs have completed. This is
    helpful when one ROI is significantly larger than other ROIs.

    Diffusion is considered complete when all pixels in a mask have non-zero heat values
    or when the creep reaches the

    Args:
        stack (numpy.ndarray): A stack of ROIs
        centroid (numpy.ndarray): The centroid for all ROIs in the stack

    Returns:
        numpy.ndarray: An image stack, where each slice represents the heat contained in
            each pixel after diffusion
    """

    xp = cupy if device == "gpu" else numpy

    assert centroid.size == stack.ndim - 1

    start_index = numpy.vstack((centroid - 3, numpy.zeros((centroid.size,)))).astype(
        int
    )
    end_index = numpy.vstack((centroid + 3, numpy.asarray((stack.shape[1:],)))).astype(
        int
    )

    box_filt = box_filt_2d if stack.ndim == 3 else box_filt_3d
    box_norm = 9 if stack.ndim == 3 else 27
    axes = tuple(range(1, stack.ndim))

    background = ~stack
    output = xp.zeros(stack.shape, dtype=xp.float32)
    old_mask = xp.zeros(stack.shape, dtype=numpy.bool_)

    incomplete = xp.ones((stack.shape[0],), dtype=bool)
    slices = [slice(None) for _ in range(stack.ndim)]
    subslices = [slice(None) for _ in range(stack.ndim)]

    slices[0] = incomplete
    subslices[0] = incomplete
    flame_index = (incomplete,) + tuple(centroid.tolist())

    iteration = 0

    # Loop until diffusion is complete
    while True:

        # Define the starting and stopping indices
        for i, (s, e) in enumerate(
            zip(numpy.max(start_index, axis=0), numpy.min(end_index, axis=0))
        ):
            # Indices for performing integral image calculations
            slices[i + 1] = slice(int(s), int(e), 1)

            # Indices for referencing back to original points
            subslices[i + 1] = slice(int(s + 2), int(e - 1), 1)

        # Create tuples for indexing
        slices_tuple = tuple(slices)
        subslices_tuple = tuple(subslices)

        start_index[0] -= 1
        end_index[0] += 1

        # Fuel the flame
        output[flame_index] += utils.DIFF_FUEL

        # Apply the mean filter using the integral image
        heat = box_filt(output[slices_tuple]) / box_norm

        b = background[subslices_tuple]
        heat[b] = 0
        output[subslices_tuple] = heat

        if (iteration % 10) == 0:

            mask = output > utils.DIFF_SMOLDER

            # In place binary operation to update list of incomplete ROIs
            # slices/subslices reference incomplete, changing the ROIs processed
            incomplete &= ~xp.all(stack == mask, axis=axes).squeeze().astype(xp.bool_)

            # If nothing is incomplete, everything is finished
            if ~incomplete.any():
                break

            # Return if diffusion has stalled
            if xp.all(old_mask[mask]):
                incomplete_args = xp.argwhere(incomplete)

                logger.debug(
                    f"Diffusion stalled for objects ({label_count + incomplete_args.squeeze()}) at iteration {iteration}. Check output for accuracy."
                )

                break

            # Shock the system, accelerates diffusion and prevents stalling
            output[mask] += utils.DIFF_SHOCK
            output[~mask] = 0

            old_mask |= mask

            # Check to see if we can shrink the size of the integral image
            indices = xp.any(mask[incomplete], axis=0)

            for i in range(start_index.shape[1]):

                axis_indices = xp.any(
                    indices,
                    axis=tuple(j for j in range(start_index.shape[1]) if j != i),
                )
                axis_indices = xp.argwhere(axis_indices).squeeze()

                start_index[0, i] = axis_indices[0] - 3
                end_index[0, i] = axis_indices[-1] + 3

        iteration += 1

    return output


def vector_norm(vector: numpy.ndarray, axis: int = -1):

    norm = (vector ** 2).sum(axis=axis)

    vector = vector / (
        numpy.expand_dims(norm, axis=axis) + numpy.finfo(numpy.float32).eps
    )

    return vector


def gradient_store(
    stack: numpy.ndarray,
    slices: List[Tuple[slice, slice]],
    subslices: List[Tuple[slice, slice]],
    centroid: numpy.ndarray,
    sc: int,
    output: numpy.ndarray,
    device: str,
) -> None:

    heat_slices = creeping_mean_filter(stack, centroid, sc, device)

    axes = tuple(range(1, stack.ndim))

    vector_slices = list(numpy.gradient(heat_slices, axis=axes))
    vector_slices = numpy.stack(vector_slices, axis=-1)
    vector_slices = vector_norm(vector_slices)
    vector_slices[~stack] = 0

    for i, s in enumerate(vector_slices):

        output[slices[sc + i]] += s[subslices[i]]

    return stack.shape[0]


def labels_to_vectors(
    masks: numpy.ndarray, name: Optional[str] = None
) -> numpy.ndarray:

    p_identity = current_process()._identity
    process_num = 0 if len(p_identity) == 0 else current_process()._identity[0] - 1
    use_gpu = (cupy.cuda.runtime.getDeviceCount() >= process_num) and utils.HAS_CUDA
    device = "gpu" if use_gpu else "cpu"
    if use_gpu:
        cupy.cuda.Device(process_num // 2).use()

    xp = cupy if use_gpu else numpy

    name = (name + ": ") if name is not None else ""

    if masks.ndim not in [2, 3]:
        message = f"Masks must be 2d or 3d. Got {int(masks.ndim)}d instead."
        logger.error(message)
        raise ValueError(message)

    # ensure unique and contiguous labels
    uniques, inverse = xp.unique(masks, return_inverse=True)
    if len(uniques) == 1:
        if uniques[0] == 0:
            logger.warning("No objects found. Returning zero flows.")
        else:
            logger.warning(
                "The masked tile/chunk contains one object and no "
                "background pixels. Returning zero flows."
            )
        return xp.zeros(shape=(masks.ndim, *masks.shape), dtype=xp.float32)

    labels = xp.reshape(inverse, masks.shape)

    if use_gpu:
        slices = scipy.ndimage.find_objects(labels.get())
    else:
        slices = scipy.ndimage.find_objects(labels)

    sc = 0

    vector = xp.zeros(masks.shape + (masks.ndim,), dtype=xp.float32)

    threads = set()
    completed = 0
    with ThreadPoolExecutor(2) as executor:
        for stack, subslices, centroid in stack_rois(labels, slices, device):

            threads.add(
                executor.submit(
                    gradient_store,
                    stack,
                    slices,
                    subslices,
                    centroid,
                    sc,
                    vector,
                    device,
                )
            )

            sc += stack.shape[0]

            # Pause every 10 iterations to let threads complete to conserve memory
            if len(threads) > 10:
                done, threads = wait(threads, return_when="FIRST_COMPLETED")
                for thread in done:
                    completed += thread.result()
                logger.info(f"{name}{100*completed/len(slices):6.2f}% complete")

        # Check that the last threads completed successfully
        for thread in as_completed(threads):
            completed += thread.result()
            logger.info(f"{name}{100*completed/len(slices):6.2f}% complete")

    vector = vector.transpose((stack.ndim - 1,) + tuple(range(stack.ndim - 1)))

    if use_gpu:
        vector = vector.get()

    return vector


def relabel(
    labels: numpy.ndarray,
    original_index: numpy.ndarray,
    flow_points: numpy.ndarray,
    device: str,
) -> Tuple[numpy.ndarray, numpy.ndarray]:

    xp = cupy if device == "gpu" else numpy

    flow_index = xp.round(flow_points).astype(xp.int32)
    flow_index = tuple(flow_index[p] for p in range(flow_index.shape[0]))

    # Relabel original points if the flow hits an object
    # Propagate the label along the flow
    new_labels = labels[flow_index]
    old_labels = xp.full(new_labels.shape, -2)

    i = 30
    while xp.any(new_labels != old_labels):
        old_labels = new_labels
        resolved = new_labels > -1
        orig_ind = tuple(p[resolved] for p in original_index)
        flow_ind = tuple(p[resolved] for p in flow_index)
        labels[orig_ind] = labels[flow_ind]
        new_labels = labels[flow_index]

    return tuple(o[~resolved] for o in original_index), flow_points[:, ~resolved]


def interpolate_flow(
    vectors: numpy.ndarray, points: numpy.ndarray, device: str
) -> numpy.ndarray:

    xp = cupy if device == "gpu" else numpy

    ndims = vectors.shape[0]

    points_floor = points.astype(xp.int32)
    points_ceil = points_floor + 1
    for d in range(ndims):
        points_ceil[d, :] = xp.clip(
            points_ceil[d, :], a_min=0, a_max=vectors.shape[d + 1] - 1
        )

    points_norm = points - points_floor
    points_norm_inv = xp.clip(
        points_ceil - points_floor - points_norm, a_min=0, a_max=None
    )

    # Shape of the local points should be:
    # N x P x (2, ) * N
    # where N is the number of dimensions and P is the number of points
    shape = points.shape + (2,) * ndims
    vector_field = xp.zeros(shape)
    for index in product(range(2), repeat=ndims):

        vector_field[(slice(None), slice(None)) + index] = vectors[
            (slice(None),)
            + tuple(
                points_floor[d] if i == 0 else points_ceil[d]
                for d, i in enumerate(index)
            )
        ]

    # Run interpolation
    for d in reversed(range(ndims)):

        if vector_field.ndim == 3:
            p = xp.zeros(vector_field.shape + (1,))
            vector_field = xp.expand_dims(vector_field, axis=2)
        else:
            p = xp.zeros(vector_field.shape[:-1] + (1,))
        p[..., 0, 0] = points_norm_inv[d]
        p[..., 1, 0] = points_norm[d]

        # Stacked matrix multiply
        vector_field = vector_field @ p

        vector_field = vector_field.squeeze(axis=-1)

    return vector_field[..., 0] + points


def vectors_to_labels(
    vector: numpy.ndarray, mask: numpy.ndarray, r: int
) -> numpy.ndarray:

    p_identity = current_process()._identity
    process_num = 0 if len(p_identity) == 0 else current_process()._identity[0] - 1

    xp = cupy if utils.HAS_CUDA else numpy
    device = "gpu" if utils.HAS_CUDA else "cpu"

    if utils.HAS_CUDA:
        cupy.cuda.Device(process_num % cupy.cuda.runtime.getDeviceCount()).use()
        vector = xp.asarray(vector)
        mask = xp.asarray(mask)

    """Step 1: Find object boundaries using local dot product"""
    # Normalize the vector
    v_norm = vector_norm(vector, axis=0)

    # Pad vectors for integral image calculations
    pad = [(0, 0)] + [[r + 1, r] for _ in range(1, vector.ndim)]
    vector_pad = xp.pad(v_norm, pad)
    mask_pad = xp.pad(mask, pad)

    # Get a local count of foreground pixels
    counts = box_filt_2d(mask_pad.astype(xp.int32), 2 * r + 1)
    counts[~mask] = 0

    # Get the local mean vector
    v_mean = box_filt_2d(vector_pad, 2 * r + 1) / counts

    # Compute the dot product between the local mean vector and the vector at a point
    v_dot = (v_norm * v_mean).sum(axis=0)
    v_div = numpy.gradient(v_norm[0], axis=0) + numpy.gradient(v_norm[1], axis=1)

    """ Step 2: Label internal objects """
    # Label cell objects without borders
    dot_internal = ((v_div < 0.0) | (v_dot >= 0.8)) & mask.squeeze()
    if utils.HAS_CUDA:
        dot_internal = dot_internal.get()
        mask = mask.get()
    dot_internal = scipy.ndimage.binary_fill_holes(
        dot_internal, numpy.ones((2 * r + 1, 2 * r + 1))
    )
    cells, _ = scipy.ndimage.label(dot_internal & mask.squeeze(), numpy.ones((3, 3)))
    if utils.HAS_CUDA:
        cells = xp.asarray(cells)
        mask = xp.asarray(mask)
        dot_internal = xp.asarray(dot_internal)
    dot_boundary = mask.squeeze() & ~dot_internal

    # If no borders, just return the labeled images
    if ~xp.any(dot_boundary):

        if utils.HAS_CUDA:
            ProcessManager.log("Return early")
            cells = cells.get()

        return cells

    """ Step 3: Follow flows from borders to objects """
    # Get starting points for interpolation at border pixels
    points_original = xp.where(dot_boundary)
    points_float = xp.asarray(points_original).astype(xp.float32)
    cells[points_original] = -1

    # Run the first iteration of flow dynamics using interpolation
    points_float += v_norm[(slice(None),) + points_original]

    # Propagate labels
    points_original, points_float = relabel(
        cells, points_original, points_float, device
    )

    # Run interpolation iterations
    factor = 1.00
    for batch in range(5):

        # Nudge the current position with the local average in case it's stuck in a well
        flow_index = xp.round(points_float).astype(xp.int32)
        flow_index = tuple(flow_index[p] for p in range(flow_index.shape[0]))

        # Follow flows for 5 steps
        for step in range(5):
            factor *= 1.10
            points_float = interpolate_flow(v_norm * factor, points_float, device)

        for d in range(points_float.ndim - 1):
            points_float[d, :] = xp.clip(
                points_float[d, :], a_min=0, a_max=v_norm.shape[d + 1] - 1
            )

        # Resolve labels
        points_original, points_float = relabel(
            cells, points_original, points_float, device
        )

        if points_float.shape[1] == 0:
            break

    if points_float.shape[1] != 0:
        logger.warning(
            f"{points_float.shape[1]} pixels in the mask were not assigned to an object. Check the output for label quality."
        )
        cells[cells == -1] = 0

    """ Step 4: Remove objects smaller than the integral image window size """
    if utils.HAS_CUDA:
        slices = scipy.ndimage.find_objects(cells.get())
    else:
        slices = scipy.ndimage.find_objects(cells)

    removed_objects = False
    for i, s in enumerate(slices):

        tile = cells[s]
        tile_mask = tile == (i + 1)

        if mask.astype(xp.int32).sum() < (2 * r + 1) ** cells.ndim:

            removed_objects = True
            tile[tile_mask] = 0
            cells[s] = tile

    if removed_objects:
        _, inverse = xp.unique(cells, return_inverse=True)
        cells = inverse.reshape(cells.shape)

    if utils.HAS_CUDA:
        cells = cells.get()

    return cells
