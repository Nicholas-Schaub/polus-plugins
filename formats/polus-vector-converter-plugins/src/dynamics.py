import logging
from typing import List
from typing import Optional
from typing import Tuple
from typing import Iterator
from concurrent.futures import ThreadPoolExecutor, wait, as_completed

import numpy
from numpy.core.fromnumeric import ndim
import scipy.ndimage.measurements
import torch.nn.functional
from numba import njit

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
    image: numpy.ndarray, slices: List[Tuple[slice, slice]]
) -> Iterator[Tuple[numpy.ndarray, numpy.ndarray, numpy.ndarray]]:
    """Stack multiple ROIs into a single n-d array

    This function stacks all ROIs along the 0th dimension such that the centroid of each
    object occurs at the same higher dimensional coordinates. For example, in the case
    of 2-dimensional images, the `image` is the original image and `slices` is a list
    of slices indicating which objects to stack. If there are N objects, then the
    centroid will occur at [:,Y,X], where [Y,X] is the centroid for every object where
    [0,:,:] is the first object, [1,:,:] is the second object, etc.

    To help conserve memory, this function is a generator. Each iteration will yield
    roughly 1/4 the square TILE_SIZE number of pixels.

    Args:
        image (numpy.ndarray): A labeled image
        slices (List[Tuple[slice, slice]]): A list of slices, one for each ROI.

    Yields:
        Iterator[Tuple[numpy.ndarray, numpy.ndarray]]: [description]
    """

    # Set the maximum number of pixels to process at a time
    max_pixels = 0.25 * utils.TILE_SIZE ** 2
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
            extent = numpy.zeros((2 * dims,))
            extent[:dims] = -geo_median
            extent[dims:] = mask.shape - geo_median

            masks.append(mask)
            extents.append(extent)

            current_label += 1
            total_pixels = max_slice_pixels * len(masks)

        # Calculate the size of output needed to store all ROIs with the same centroid
        extents = numpy.asarray(extents).astype(int)
        centroid = -numpy.min(extents[:, :dims], axis=0) + 2
        shape = numpy.max(extents[:, dims:], axis=0) + centroid + 1

        # Calculate offsets for each ROI to position the centroid at the same location
        extents[:, :dims] += centroid
        extents[:, dims:] += centroid

        # Create the output, and stack the ROIs
        stack = numpy.zeros(
            [len(masks)] + shape.astype(int).tolist(), dtype=numpy.bool_
        )

        sub_slices = []

        for i, mask in enumerate(masks):

            ext = extents[i]

            slice_tuple = tuple(slice(ext[i], ext[i + dims]) for i in range(dims))

            sub_slices.append(slice_tuple)

            stack[(i,) + slice_tuple] = mask

        yield stack, sub_slices, centroid


def mean_filt_2d(image):

    integral_image = image.cumsum(axis=1).cumsum(axis=2)
    mean_filt = (
        integral_image[:, 3:, 3:]
        - integral_image[:, 3:, :-3]
        - integral_image[:, :-3, 3:]
        + integral_image[:, :-3, :-3]
    ) / 9

    return mean_filt


def mean_filt_3d(image):

    integral_image = image.cumsum(axis=1).cumsum(axis=2).cumsum(axis=3)
    mean_filt = (
        integral_image[:, 3:, 3:, 3:]
        + integral_image[:, 3:, :-3, :-3]
        + integral_image[:, :-3, 3:, :-3]
        + integral_image[:, :-3, :-3, 3:]
        - integral_image[:, :-3, :-3, :-3]
        - integral_image[:, :-3, 3:, 3:]
        - integral_image[:, 3:, :-3, 3:]
        - integral_image[:, 3:, 3:, :-3]
    ) / 27

    return mean_filt


def creeping_mean_filter(
    stack: numpy.ndarray, centroid: numpy.ndarray, label_count: int
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

    assert centroid.size == stack.ndim - 1

    start_index = numpy.vstack((centroid - 3, numpy.zeros((centroid.size,)))).astype(
        int
    )
    end_index = numpy.vstack((centroid + 3, numpy.asarray((stack.shape[1:],)))).astype(
        int
    )

    mean_filt = mean_filt_2d if stack.ndim == 3 else mean_filt_3d
    axes = tuple(range(1, stack.ndim))

    background = ~stack
    output = numpy.zeros(stack.shape, dtype=numpy.float32)
    old_mask = numpy.zeros(stack.shape, dtype=numpy.bool_)

    incomplete = numpy.ones((stack.shape[0],), dtype=bool)
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
        heat = mean_filt(output[slices_tuple])

        b = background[subslices_tuple]
        heat[b] = 0
        output[subslices_tuple] = heat

        if (iteration % 10) == 0:

            mask = output > utils.DIFF_SMOLDER

            # In place binary operation to update list of incomplete ROIs
            # slices/subslices reference incomplete, changing the ROIs processed
            incomplete &= (
                ~numpy.all(stack == mask, axis=axes).squeeze().astype(numpy.bool_)
            )

            # If nothing is incomplete, everything is finished
            if ~incomplete.any():
                break

            # Return if diffusion has stalled
            if numpy.all(old_mask[mask]):
                incomplete_args = numpy.argwhere(incomplete)

                logger.debug(
                    f"Diffusion stalled for objects ({label_count + incomplete_args.squeeze()}) at iteration {iteration}. Check output for accuracy."
                )

                break

            # Shock the system, accelerates diffusion and prevents stalling
            output[mask] += utils.DIFF_SHOCK
            output[~mask] = 0

            old_mask |= mask

            # Check to see if we can shrink the size of the integral image
            indices = numpy.any(mask[incomplete], axis=0)

            for i in range(start_index.shape[1]):

                axis_indices = numpy.any(
                    indices,
                    axis=tuple(j for j in range(start_index.shape[1]) if j != i),
                )
                axis_indices = numpy.argwhere(axis_indices).squeeze()

                start_index[0, i] = axis_indices[0] - 3
                end_index[0, i] = axis_indices[-1] + 3

        iteration += 1

    return output


def gradient_store(
    stack: numpy.ndarray,
    slices: List[Tuple[slice, slice]],
    subslices: List[Tuple[slice, slice]],
    centroid: numpy.ndarray,
    sc: int,
    output: numpy.ndarray,
) -> None:

    heat_slices = creeping_mean_filter(stack, centroid, sc)

    axes = tuple(range(1, stack.ndim))

    vector_slices = list(numpy.gradient(heat_slices, axis=axes))
    vector_slices = numpy.stack(vector_slices, axis=stack.ndim)
    vector_slices = vector_slices / (
        numpy.linalg.norm(vector_slices, axis=stack.ndim)[..., numpy.newaxis]
        + numpy.finfo(numpy.float32).eps
    )
    vector_slices[~stack] = 0

    for i, s in enumerate(vector_slices):

        output[slices[sc + i]] += s[subslices[i]]

    return stack.shape[0]


def masks_to_vectors(masks: numpy.ndarray, name: Optional[str] = None) -> numpy.ndarray:

    name = (name + ": ") if name is not None else ""

    if masks.ndim not in [2, 3]:
        message = f"Masks must be 2d or 3d. Got {int(masks.ndim)}d instead."
        logger.error(message)
        raise ValueError(message)

    # ensure unique and contiguous labels
    uniques, inverse = numpy.unique(masks, return_inverse=True)
    if len(uniques) == 1:
        if uniques[0] == 0:
            logger.warning("No objects found. Returning zero flows.")
        else:
            logger.warning(
                "The masked tile/chunk contains one object and no "
                "background pixels. Returning zero flows."
            )
        return numpy.zeros(shape=(masks.ndim, *masks.shape), dtype=numpy.float32)
    labels = numpy.reshape(inverse, masks.shape)

    slices = scipy.ndimage.find_objects(labels)
    sc = 0

    vector = numpy.zeros(masks.shape + (masks.ndim,), dtype=numpy.float32)

    threads = set()
    completed = 0
    with ThreadPoolExecutor(2) as executor:
        for stack, subslices, centroid in stack_rois(labels, slices):

            threads.add(
                executor.submit(
                    gradient_store, stack, slices, subslices, centroid, sc, vector
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

    return vector


@njit("float32[:,:,:], float32[:,:,:], uint32[:,:], uint32", nogil=True)
def _euler_integration_2d(
    locations: numpy.ndarray,
    flows: numpy.ndarray,
    indices: numpy.ndarray,
    num_iterations: numpy.uint32,
):
    """Applies numerical integration of the given flows to find where each
     pixel will converge.

    Args:
        locations: (2, Y, X) array of pixel locations. This starts off as a
                    meshgrid and we overwrite it with the coordinates where the
                    flow for each pixel converged.
        flows: (2, Y, X) array of gradients to integrate over.
        indices: (num_pixels, 2) array of masked pixels for which to integrate
                  flows.
        num_iterations: Number of steps to run the numerical integration.
    """
    shape_y, shape_x = flows.shape[1:]
    for _ in range(num_iterations):
        for i in range(indices.shape[0]):
            y, x = indices[i, 0], indices[i, 1]

            # move the current location along the flow to a new location and
            # make sure each new index is within the bounds of the image.
            curr_y, curr_x = int(locations[0, y, x]), int(locations[1, y, x])
            flow_y, flow_x = flows[0, curr_y, curr_x], flows[1, curr_y, curr_x]
            next_y, next_x = locations[0, y, x] - flow_y, locations[1, y, x] - flow_x
            next_y, next_x = min(shape_y - 1, max(0, next_y)), min(
                shape_x - 1, max(0, next_x)
            )

            locations[0, y, x] = next_y
            locations[1, y, x] = next_x
    return locations


@njit("float32[:,:,:,:], float32[:,:,:,:], uint32[:,:], uint32", nogil=True)
def _euler_integration_3d(
    locations: numpy.ndarray,
    flows: numpy.ndarray,
    indices: numpy.ndarray,
    num_iterations: numpy.uint32,
):
    """Applies numerical integration of the given flows to find where each
     pixel will converge.

    Args:
        locations: (3, Z, Y, X) array of pixel locations. This starts off as a
                    meshgrid and we overwrite it with the coordinates where the
                    flow for each pixel converged.
        flows: (3, Z, Y, X) array of gradients to integrate over.
        indices: (num_pixels, 3) array of masked pixels for which to integrate
                  flows.
        num_iterations: Number of steps to run the numerical integration.
    """
    shape_z, shape_y, shape_x = flows.shape[1:]
    for _ in range(num_iterations):
        for i in range(indices.shape[0]):
            z, y, x = indices[i, 0], indices[i, 1], indices[i, 2]

            curr_z, curr_y, curr_x = (
                int(locations[0, z, y, x]),
                int(locations[1, z, y, x]),
                int(locations[2, z, y, x]),
            )
            flow_z, flow_y, flow_x = (
                flows[0, curr_z, curr_y, curr_x],
                flows[1, curr_z, curr_y, curr_x],
                flows[2, curr_z, curr_y, curr_x],
            )
            next_z, next_y, next_x = (
                locations[0, z, y, x] - flow_z,
                locations[1, z, y, x] - flow_y,
                locations[2, z, y, x] - flow_x,
            )

            next_z = min(shape_z - 1, max(0, next_z))
            next_y = min(shape_y - 1, max(0, next_y))
            next_x = min(shape_x - 1, max(0, next_x))

            locations[0, z, y, x] = next_z
            locations[1, z, y, x] = next_y
            locations[2, z, y, x] = next_x
    return locations


def _interpolate_torch(
    locations: numpy.ndarray,
    flows: numpy.ndarray,
    num_iterations: int,
    device: int,
):
    """Runs follows 2d and 3d flows with interpolation using the indexed GPU.

    Args:
        locations: (2, Y, X)/(3, Z, Y, X) array of pixel locations. This starts
                    off as a meshgrid and we overwrite it with the coordinates
                    where the flow for each pixel converged.
        flows: (2, Y, X)/(3, Z, Y, X) array of gradients to integrate over.
        num_iterations: Number of steps to run the numerical integration.
        device: Index of GPU to by used.
    """
    ndims, shape = flows.shape[0], flows.shape[1:]
    logger.debug(f"Interpolating flows of shape {shape} on GPU {device}...")

    # `locations` grid needs to be shaped (N, Y, X, 2)/(N, Z, Y, X, 3) for `grid_sample` method.
    indices_list = list(reversed(range(ndims)))
    locations = torch.tensor(
        locations[indices_list].T, device=f"cuda:{device}", dtype=torch.float32
    )
    locations = locations.view(1, *tuple(locations.shape))
    # `locations` needs to be in the [-1, 1] square/cube grid.
    for k in range(ndims):
        locations[..., k] = locations[..., k] / (shape[ndims - 1 - k] - 1)
    locations = locations * 2 - 1

    # `flows` input needs to be shaped (N, C, Y, X)/(N, C, Z, Y, X) for `grid_sample` method.
    flows = torch.tensor(
        flows[indices_list], device=f"cuda:{device}", dtype=torch.float32
    )
    flows = flows.view(1, *tuple(flows.shape))

    for k in range(ndims):
        flows[:, k, ...] /= (shape[ndims - 1 - k] - 1) / 2.0

    logger.debug(f"Following flows on GPU {device} for {num_iterations} steps...")
    for _ in range(num_iterations):
        interpolated_flows = torch.nn.functional.grid_sample(
            flows, locations, align_corners=True
        )
        for k in range(ndims):
            locations[..., k] = torch.clamp(
                locations[..., k] - interpolated_flows[:, k, ...],
                -1.0,
                1.0,
            )

    # rescale locations to (Z, Y, X) grid
    locations = (locations + 1) * 0.5
    for k in range(ndims):
        locations[..., k] = locations[..., k] * (shape[ndims - 1 - k] - 1)

    logger.debug("moving converged locations to cpu...")
    locations = locations[..., indices_list].cpu().numpy().squeeze().T
    return locations


@njit("float32[:,:,:], float32[:], float32[:], float32[:,:]", nogil=True)
def _interpolate_flows_2d(
    flows: numpy.ndarray,
    y_indices: numpy.ndarray,
    x_indices: numpy.ndarray,
    interpolated_flows: numpy.ndarray,
):
    """Follow flows using bilinear interpolation.

    Args:
        flows: (2, Y, X) array of flows to follow
        y_indices: (num_indices, ) array of y-indices of pixels to follow.
        x_indices: (num_indices, ) array of x-indices of pixels to follow.
        interpolated_flows: array where the intermediate interpolated flows will
                             be stored.
    """
    num_channels, shape_y, shape_x = flows.shape

    y_floor = numpy.asarray(y_indices, dtype=numpy.uint32)
    x_floor = numpy.asarray(x_indices, dtype=numpy.uint32)

    # These need to be in the range [0, 1).
    y_indices = y_indices - y_floor
    x_indices = x_indices - x_floor

    for i in range(y_floor.shape[0]):
        # The top-left grid point (in matrix notation)
        y0, x0 = min(shape_y - 1, max(0, y_floor[i])), min(
            shape_x - 1, max(0, x_floor[i])
        )
        # The bottom-right grid point
        y1, x1 = min(shape_y - 1, y0 + 1), min(shape_x - 1, x0 + 1)
        # The query point
        y, x = y_indices[i], x_indices[i]

        for channel in range(num_channels):
            interpolated_flows[channel, i] = (
                flows[channel, y0, x0] * (1 - y) * (1 - x)
                + flows[channel, y0, x1] * (1 - y) * x
                + flows[channel, y1, x0] * y * (1 - x)
                + flows[channel, y1, x1] * y * x
            )
    return


# noinspection PyUnusedLocal
@njit("float32[:,:,:,:], float32[:], float32[:], float32[:], float32[:,:]", nogil=True)
def _interpolate_flows_3d(
    flows: numpy.ndarray,
    z_indices: numpy.ndarray,
    y_indices: numpy.ndarray,
    x_indices: numpy.ndarray,
    interpolated_flows: numpy.ndarray,
):
    """Follow flows using trilinear interpolation.

    TODO: This function was a stretch goal for the sprint and has not yet been completed.

    Args:
        flows: (3, Z, Y, X) array of flows to follow
        z_indices: (num_indices, ) array of y-indices of pixels to follow.
        y_indices: (num_indices, ) array of y-indices of pixels to follow.
        x_indices: (num_indices, ) array of x-indices of pixels to follow.
        interpolated_flows: array where the intermediate interpolated flows will
                             be stored.
    """
    num_channels, shape_z, shape_y, shape_x = flows.shape

    z_floor = numpy.asarray(z_indices, dtype=numpy.uint32)
    y_floor = numpy.asarray(y_indices, dtype=numpy.uint32)
    x_floor = numpy.asarray(x_indices, dtype=numpy.uint32)

    # These need to be in the range [0, 1).
    z_indices = z_indices - z_floor
    y_indices = y_indices - y_floor
    x_indices = x_indices - x_floor

    for i in range(z_floor.shape[0]):
        # The top-left grid point (in matrix notation)
        z0 = min(shape_z - 1, max(0, z_floor[i]))
        y0 = min(shape_y - 1, max(0, y_floor[i]))
        x0 = min(shape_x - 1, max(0, x_floor[i]))

        # The bottom-right grid point
        z1 = min(shape_z - 1, z0 + 1)
        y1 = min(shape_y - 1, y0 + 1)
        x1 = min(shape_x - 1, x0 + 1)

        # The query point
        z, y, x = z_indices[i], y_indices[i], x_indices[i]

        for channel in range(num_channels):
            # TODO: Implement equations from https://en.wikipedia.org/wiki/Trilinear_interpolation
            interpolated_flows[channel, i] = 0
            raise NotImplementedError
    return


def _interpolate_flows_cpu(
    locations: numpy.ndarray,
    flows: numpy.ndarray,
    num_iterations: numpy.uint32,
) -> numpy.ndarray:
    """Interpolate flows with the CPU using jit-compiled functions.

    Args:
        locations: Array of pixel indices where flows are to be interpolated
        flows: Array of flows which need to be interpolated.
        num_iterations: Number of iterations to follow flows.

    Returns:
        Array of locations where the flow from each pixel converged.
    """
    ndims, shape = int(flows.shape[0]), flows.shape[1:]
    function = _interpolate_flows_2d if ndims == 2 else _interpolate_flows_3d
    interpolated_flows = numpy.zeros_like(locations, dtype=numpy.float32)
    for _ in range(num_iterations):
        function(flows, *(locations[i] for i in range(ndims)), interpolated_flows)
        for axis in range(ndims):
            locations[axis] = numpy.clip(
                locations[axis] - interpolated_flows[axis],
                a_min=0,
                a_max=shape[axis] - 1,
            )
    return locations


# TODO: Figure out how to determine a good number of iterations. Perhaps by
#  checking pixel locations against flow-magnitudes? Unlikely due to unit-norm
#  of flows.
def follow_flows(
    flows: numpy.ndarray,
    *,
    num_iterations: Optional[int] = None,
    interpolate: bool = True,
    device: Optional[int] = None,
) -> numpy.ndarray:
    """Run flow-field dynamics to recover masks in 2d and 3d.

    We initialize the pixel locations using a meshgrid and then follow the flow
    for each pixel location to find where it converges.

    Args:
        flows: Flow-fields of shape (2, Y, X) or (3, Z, Y, X) of type float32.
        num_iterations: The number of steps for which to follow the flows. If
                         None, we follow flows until they have approximately
                         converged.
        interpolate: Whether to interpolate flows while running dynamics.
        device: If None, run the dynamics on the CPU. Otherwise, the index of
                 the GPU to use.

    Returns:
        Converged locations of each pixel. Shape (2, Y, X) or (3, Z, Y, X).
    """
    shape, ndims = flows.shape[1:], flows.ndim - 1

    # TODO: Remove this after implementing 3d interpolation using the cpu
    if device is None and ndims == 3:
        interpolate = False

    # initialize pixel-locations to a meshgrid
    locations = numpy.asarray(
        numpy.meshgrid(*(numpy.arange(d) for d in shape), indexing="ij"),
        dtype=numpy.float32,
    )

    # Get indices of all non-zero flow locations
    flow_magnitudes = numpy.sum(numpy.square(flows), axis=0)
    indices = numpy.asarray(
        numpy.nonzero(flow_magnitudes > 1e-3),
        dtype=numpy.uint32,
    ).T

    if device is None:  # cpu
        num_iterations = numpy.uint32(num_iterations)
        if interpolate:
            query_indices = tuple(indices[:, dim] for dim in range(flows.ndim - 1))
            if flows.ndim == 3:
                locations[
                    :, query_indices[0], query_indices[1]
                ] = _interpolate_flows_cpu(
                    locations[:, query_indices[0], query_indices[1]],
                    flows,
                    num_iterations,
                )
            else:
                locations[
                    :, query_indices[0], query_indices[1], query_indices[2]
                ] = _interpolate_flows_cpu(
                    locations[:, query_indices[0], query_indices[1], query_indices[2]],
                    flows,
                    num_iterations,
                )
        else:
            function = (
                _euler_integration_2d if flows.ndim == 3 else _euler_integration_3d
            )
            locations = function(locations, flows, indices, num_iterations)
    else:  # gpu
        locations = _interpolate_torch(locations, flows, num_iterations, device)
    return locations


def _compute_convergence_histograms(
    locations: numpy.ndarray,
    edge_padding: int,
) -> Tuple[List[numpy.ndarray], numpy.ndarray, numpy.ndarray]:
    """Compute nd-histograms of converged pixel locations.

    Args:
        locations: Array of pixel locations after following flows.
        edge_padding: histogram edge padding.

    Returns:
        A tuple of:
            * flattened_flows: These are used to build the histograms and later
                                used to assign the masked regions.
            * histogram: An nd-histogram of the frequency with which each final
                          location was converged upon.
            * spread_histogram: Same as `histogram` except that the peaks have
                                 been spread out to allow merging of nearby
                                 locations of convergence.
    """
    ndims, masks_shape = locations.shape[0], locations.shape[1:]
    logger.debug("computing convergence histograms...")

    flattened_flows = [
        numpy.asarray(locations[dim], dtype=numpy.int32).flatten()
        for dim in range(ndims)
    ]
    edges = [
        numpy.arange(-0.5 - edge_padding, masks_shape[dim] + 0.5 + edge_padding, 1)
        for dim in range(ndims)
    ]

    histogram, _ = numpy.histogramdd(tuple(flattened_flows), bins=edges)

    spread_histogram = histogram.copy()
    for dim in range(ndims):
        spread_histogram = scipy.ndimage.maximum_filter1d(spread_histogram, 5, axis=dim)

    return flattened_flows, histogram, spread_histogram


def _compute_expansion_indices(ndims: int) -> numpy.ndarray:
    """Computes a (3, 3) or (3, 3, 3) array of difference in indices from a
     pixel to its neighbors.

    Args:
        ndims: The dimensionality of masks to be computed.

    Returns:
        A nd-array of index differences.
    """
    logger.debug("computing expansion indices...")
    expansion_indices = numpy.nonzero(numpy.ones(tuple([3] * ndims)))
    expansion_indices = numpy.asarray(
        [numpy.expand_dims(e, 1) for e in expansion_indices]
    )
    expansion_indices -= 1
    return expansion_indices


def _compute_seed_indices(
    histogram: numpy.ndarray, max_histogram: numpy.ndarray
) -> List[numpy.ndarray]:
    """Find the indices of the most frequently converged upon locations.

    Args:
        histogram: from `_compute_convergence_histograms`
        max_histogram: from `_compute_convergence_histograms`

    Returns:
        A list of two or three arrays representing the [y, x] or [z, y, x]
         indices of the seeds from which to start merging nearby convergent
         locations.
    """
    logger.debug("computing seed indices...")
    seeds = numpy.nonzero(
        numpy.logical_and(
            histogram - max_histogram > -1e-6,
            histogram > 10,
        )
    )

    seed_frequencies = histogram[seeds]
    seed_sorting_indices = numpy.argsort(seed_frequencies)[::-1]
    seeds = [s[seed_sorting_indices] for s in seeds]
    return list(numpy.asarray(seeds).transpose())


def _get_masks_from_histograms(
    locations: numpy.ndarray, edge_padding: int
) -> numpy.ndarray:
    """Uses nd-histograms, as per cellpose, to merge nearby convergent flows
     and produce labelled masks.

    Does this work? See James 1:6.
    How does this work? You, dear developer, can attempt to figure it out from
     cellpose's code. I have renamed the variables to the best of my
     understanding. During your attempt, do increment the following counters:
        * number-of-wasted-developer-hours: 16
        * measure-of-lost-developer-sanity: 33 %
    If you do succeed in figuring this out, please refactor the nested loops
     into an intuitive logical flow.

    Args:
        locations: 2d or 3d array of convergent locations after following flows.
        edge_padding: histogram edge padding.

    Returns:
        Array of labelled masks.
    """
    ndims = int(locations.shape[0])
    flattened_flows, histogram, spread_histogram = _compute_convergence_histograms(
        locations, edge_padding
    )
    expansion_indices = _compute_expansion_indices(ndims)
    seed_indices = _compute_seed_indices(histogram, spread_histogram)
    logger.debug(
        f"using histograms to recover masks for {len(seed_indices)} convergent locations..."
    )

    seed_indices = list(map(list, seed_indices))
    for _ in range(5):
        for seed_index in range(len(seed_indices)):
            expanded_pixel_indices = [
                numpy.asarray(
                    e[:] + numpy.expand_dims(seed_indices[seed_index][dim], 0)
                ).flatten()
                for dim, e in enumerate(expansion_indices)
            ]
            is_index_in_mask = [
                numpy.logical_and(
                    expanded_pixel_index >= 0,
                    expanded_pixel_index < histogram.shape[dim],
                )
                for dim, expanded_pixel_index in enumerate(expanded_pixel_indices)
            ]
            is_index_in_mask = numpy.all(tuple(is_index_in_mask), axis=0)

            # remove all expanded pixels that are not in the mask and all pixels where
            # 2 or less other pixels converged after following flows
            expanded_pixel_indices = tuple(
                (pixels[is_index_in_mask] for pixels in expanded_pixel_indices)
            )
            did_enough_flows_converge = histogram[expanded_pixel_indices] > 2

            # expand the each seed to the pixels where other pixels converged.
            for dim in range(ndims):
                seed_indices[seed_index][dim] = expanded_pixel_indices[dim][
                    did_enough_flows_converge
                ]
    seed_indices = list(map(tuple, seed_indices))

    # apply labels to centers
    mask_centers = numpy.zeros(histogram.shape, numpy.uint32)
    for seed_index in range(len(seed_indices)):
        mask_centers[seed_indices[seed_index]] = 1 + numpy.uint32(seed_index)

    # extract masks
    for dim in range(ndims):
        flattened_flows[dim] = flattened_flows[dim] + edge_padding
    return numpy.asarray(mask_centers[tuple(flattened_flows)], dtype=numpy.uint32)


def _remove_bad_flow_masks(
    masks: numpy.ndarray,
    flows: numpy.ndarray,
    flow_error_threshold: float,
    device: Optional[int],
) -> numpy.ndarray:
    """Remove masked objects whose recreated flows have high error.

    We begin by recreating the flows from the masks we just computed. These
     flows are compared against the flows given by the user. If the error (MSE)
     between the two flows for a masked object is larger than the given
     threshold, we remove that masked object.

    Args:
        masks: The labelled masks computed by following flows.
        flows: The original flows given by the user.
        flow_error_threshold: MSE threshold above which a masked object is
                               removed.
        device: None if using the CPU. Otherwise, the index of the GPU to use.

    Returns:
        Labelled masks which have low flow error.
    """
    logger.debug(
        f"removing bad flows with error higher than {flow_error_threshold:.2e}..."
    )
    reconstructed_flows = masks_to_vectors(masks)

    # difference between predicted flows vs mask flows
    max_mask = numpy.max(masks)
    flow_errors = numpy.zeros(max_mask)
    for i in range(reconstructed_flows.shape[0]):
        flow_errors += scipy.ndimage.mean(
            (reconstructed_flows[i] - flows[i]) ** 2,
            labels=masks,
            index=numpy.arange(1, max_mask + 1),
        )

    bad_indices = 1 + (flow_errors > flow_error_threshold).nonzero()[0]
    masks[numpy.isin(masks, bad_indices)] = 0
    return masks


def fill_holes_and_remove_small_masks(
    masks: numpy.ndarray, min_size: int = 15
) -> numpy.ndarray:
    """Discard masks smaller than `min_size` and fill any holes inside masks.

    Holes are filled using scipy.ndimage.binary_fill_holes.

    Args:
        masks: (Y, X) or (Z, Y, X) array of labelled masks. Background pixels are labelled 0.
        min_size: Minimum sized mask to keep. Set to -1 to not remove any masks.

    Returns:
        Array of the same shape as `masks` with holes filled and small masks removed.
    """
    slices = scipy.ndimage.find_objects(masks)

    label = 1
    for i, s in enumerate(slices, start=1):
        if s is None:
            continue

        object_mask = numpy.asarray(masks[s] == i)
        num_pixels = numpy.sum(object_mask)
        if 0 < min_size and num_pixels < min_size:
            masks[s][object_mask] = 0
        else:
            if masks.ndim == 3:
                for z in range(object_mask.shape[0]):
                    object_mask[z] = scipy.ndimage.binary_fill_holes(object_mask[z])
            else:
                object_mask = scipy.ndimage.binary_fill_holes(object_mask)

            masks[s][object_mask] = label
            label += 1
    return masks


def get_masks(
    locations: numpy.ndarray,
    *,
    is_cell: Optional[numpy.ndarray] = None,
    edge_padding: int = 20,
    flows: Optional[numpy.ndarray] = None,
    flow_error_threshold: Optional[float] = 0.4,
    mask_size_threshold: Optional[float] = None,
    device: Optional[int] = None,
) -> numpy.ndarray:
    """Create masks from the convergent pixel locations after following flows.

    Args:
        locations: (2, Y, X) or (3, Z, Y, X) array of converged pixel locations.
        is_cell: Optional array with the same shape as the masks representing
                  whether each pixel is inside a cell to be labelled.
        edge_padding: histogram edge padding.
        flows: flows which were followed to find `locations`. This is used to
                remove masked objects with inconsistent flows.
        flow_error_threshold: If not None, the MSE above which masked objects
                               are discarded.
        mask_size_threshold: If not None, if a masked object covers more pixels
                              than this fraction of the full size of the image.
        device: None if using the CPU. Otherwise, the index of the GPU to use.

    Returns:
        Array of labelled masks shaped (Y, X) or (Z, Y, X).
    """
    ndims, shape = locations.shape[0], locations.shape[1:]
    logger.debug(f"recovering masks of shape {shape}...")

    # pixels that are not in a cell are to remain where they start.
    if is_cell is not None:
        indices = numpy.meshgrid(
            *(numpy.arange(shape[dim]) for dim in range(ndims)), indexing="ij"
        )
        for dim in range(ndims):
            locations[dim, ~is_cell] = indices[dim][~is_cell]

    # The function contained here contains some cellpose magic
    masks = _get_masks_from_histograms(locations, edge_padding)

    # If a mask is too big relative to the size of the image, assume that it's actually background
    if mask_size_threshold is not None and mask_size_threshold > 0:
        size_threshold = int(numpy.prod(shape) * mask_size_threshold)
        _, counts = numpy.unique(masks, return_counts=True)
        for label in numpy.nonzero(counts > size_threshold)[0]:
            masks[masks == label] = 0

    labels, masks = numpy.unique(masks, return_inverse=True)
    logger.debug(f"Recovered {len(labels)} masks...")
    masks = numpy.reshape(masks, shape)

    # If the original flows are given, use them to remove masks with a high flow-error.
    if (
        len(labels) > 0
        and flow_error_threshold is not None
        and flow_error_threshold > 0
        and flows is not None
    ):
        masks = _remove_bad_flow_masks(masks, flows, flow_error_threshold, device)
        _, masks = numpy.unique(masks, return_inverse=True)
        masks = numpy.asarray(numpy.reshape(masks, shape), dtype=numpy.uint32)

    return masks
