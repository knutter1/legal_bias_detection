# Legal Bias Detection

This repository contains the code used for my bachelor's thesis, **"Automated Detection of Linguistic Biases in Swiss Court Rulings - An Approach with the help of an Open-Source-LLM and subsequent human Annotation."**

**⚠️ IMPORTANT NOTE:** The documentation for this project is currently in the process of being properly created. This repository contains the exact code that ran on my personal machines to execute the experiments for my thesis. The code is provided as a reference to demonstrate the end-to-end pipeline, from data collection to LLM inference and the annotation interface. It is not yet refactored for general reusability.

## Project Pipeline Overview

The project pipeline consists of three main stages, each handled by a dedicated set of scripts:

1.  **Data Collection:**
    * This stage involved downloading raw court ruling data from `https://www.entscheidsuche.ch/docs/`.
    * This process was executed on my personal PC.

2.  **LLM Inference & Database Management:**
    * This stage involved running the language model (LLM) inference on the collected data.
    * This was performed on a GPU server.

3.  **Human Annotation:**
    * A web-based interface was used for subsequent human annotation of the LLM's output.

## Key Files

Here is a brief overview of the most important files in this repository:

* **`grab_text.py`**:
    This script is responsible for downloading HTML files containing court ruling data and their associated metadata from `https://www.entscheidsuche.ch/docs/`. It extracts the raw text from these files and writes it into a MongoDB database.

* **`prepare_data.py`**:
    This script handles the selection of a suitable data sample for the experiment from the collected data in the MongoDB.

* **`check_bias.py`**:
    This script orchestrates the LLM inference process, running the checks on the GPUs and interacting with the database to store the model's responses.

* **`annotation_handler.py`**:
    This is a Flask service that runs the web application for human annotation. It uses Jinja2 templates to render the annotation interface.

## Next Steps

This codebase will be refactored to improve its structure, modularity, and reusability, making it easier for others to adapt and use for similar tasks.
