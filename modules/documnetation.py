
import os
import subprocess


def initialize_sphinx(docs_dir):
    """
    Initialize Sphinx in the specified directory.
    """
    subprocess.run(["sphinx-quickstart", "-q", "-p",
                   "DataPipelineDocs", "-a", "Samuel Kwame Dassi", docs_dir])


def generate_html_documentation(docs_dir):
    """
    Generate HTML documentation using Sphinx.
    """
    source_dir = os.path.join(docs_dir, "source")
    build_dir = os.path.join(docs_dir, "_build", "html")
    subprocess.run(["sphinx-build", "-b", "html", source_dir, build_dir])


def generate_documentation(project_dir):
    """
    Generate documentation for the data pipeline project using Sphinx.
    """
    # Create a directory for Sphinx documentation
    docs_dir = os.path.join(project_dir, "documentation")
    os.makedirs(docs_dir, exist_ok=True)

    # Initialize Sphinx in the docs directory
    initialize_sphinx(docs_dir)

    # Generate reStructuredText (.rst) files for each Python module
    source_dir = os.path.join(docs_dir, "source")
    for root, _, files in os.walk(os.path.join(project_dir, "modules")):
        for file_name in files:
            if file_name.endswith(".py"):
                module_path = os.path.relpath(
                    os.path.join(root, file_name), project_dir)
                subprocess.run(
                    ["sphinx-apidoc", "-o", source_dir, project_dir, module_path])

    # Generate HTML documentation
    generate_html_documentation(docs_dir)

    print("Documentation generated successfully.")


if __name__ == "__main__":
    project_dir = "e-commerce-data-pipeline"
    generate_documentation(project_dir)
