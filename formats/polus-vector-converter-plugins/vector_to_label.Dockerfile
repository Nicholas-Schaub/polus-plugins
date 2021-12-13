FROM vector-label-setup

# Default command. Additional arguments are provided through the command line
ENTRYPOINT ["python3", "vector_to_label.py"]