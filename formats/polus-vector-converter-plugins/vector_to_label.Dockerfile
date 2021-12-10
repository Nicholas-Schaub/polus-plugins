FROM labshare/polus-label-to-vector-plugin:0.7.0

# Default command. Additional arguments are provided through the command line
ENTRYPOINT ["python3", "vector_to_label.py"]