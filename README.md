# Scaling up empirical research to bigger data with Python

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/antonbabkin/ds-bazaar-workshop/HEAD?urlpath=lab)

Workshop materials for the [2021 Data Science Research Bazaar](https://datascience.wisc.edu/data-science-research-bazaar/) at UW-Madison.

This workshop is intended for researchers who have experience in analyzing data that comfortably fits in memory but are interested in scaling up to bigger than memory datasets. The following topics will be covered: measuring performance and memory usage; sampling and split-apply-combine strategy; data type optimization; efficient storage with parquet; simple parallelization; introduction to Dask. Participants interested in following along will be provided with an example dataset and instructions on setting up a programming environment. All workshop materials will be publicly available in this GitHub repository. A [prerequisite exercise](prereq.ipynb) should give you an idea of expected prior knowledge of Python and pandas.

If you are new to Python, I recommend reading about [Jupyter](https://jupyter.org/) and [pandas](https://pandas.pydata.org/). [QuantEcon](https://quantecon.org/lectures/) Python lectures are also a good resource for beginners.


## Setup

You need a running Jupyter server in order to work with workshop notebooks. The easiest way is to launch a free cloud instance in [Binder](https://mybinder.org/). A more difficult (but potentially more reliable) alternative is to create [conda](https://docs.conda.io/en/latest/) Python environment on your local computer.

### Using Binder

Click this [link](https://mybinder.org/v2/gh/antonbabkin/ds-bazaar-workshop/HEAD?urlpath=lab) to launch a new Binder instance and connect to it from your browser, then open and run the [setup notebook](setup.ipynb) to test the environment and download data. Normal launch time is under 30 seconds, but it might take longer if the repository has been recently updated, because Binder will need to rebuild the environment from scratch.

Notice that Binder platform provides computational resources for free, and so limitations are in place and availability can not be guaranteed. Read [here](https://mybinder.readthedocs.io/en/latest/about/about.html#using-the-mybinder-org-service) about usage policy and available resources.


### Local Python

This method requires some experience or readiness to read documentation. As reward, you will have persistent environment under your control that does not depend on cloud service availability. This is also a typical way to set up Python for data work.

1. Download and install [miniconda](https://docs.conda.io/en/latest/miniconda.html) (Python 3), following instructions for your operating system.

2. Open terminal (Anaconda Prompt on Windows) and clone this repository in a folder of your choice (`git clone https://github.com/antonbabkin/ds-bazaar-workshop.git`). Alternatively, [download](https://github.com/antonbabkin/ds-bazaar-workshop/archive/main.zip) and unpack repository code as ZIP.

3. In the terminal, navigate to the repository folder (`cd ds-bazaar-workshop`) and create new [conda environment](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#creating-an-environment-from-an-environment-yml-file) (`conda env create`). Environment specification will be read from the `environment.yml` file, all required packages will be downloaded and installed.

4. Activate the environment and start JupyterLab server. This will start a new Jupyter server and open Jupyter interface in browser window.
```
conda activate ds-bazaar-workshop
jupyter lab
```

5. In Jupyter, open and run the [setup notebook](setup.ipynb) to test the environment and download data.


## Data

Run cells of the [setup notebook](setup.ipynb) to download data into your environment.

The core dataset used in examples is a synthetic fake, generated from annual historical snapshops of InfoGroup data. InfoGroup is a proprietary database of all businesses in the US, available to University of Wisconsin researchers.

Synthetic version (SynIG) provides a subset of core variables and was generated from the original data using a combination of random fake data, modeling, random sampling, record shuffling and noise infusion to protect original data confidentiality. It has the same format and some resemblance of the original (eg. cross-sectional distribution of establishments and employment across states and sectors) and is suitable for educational purposes or methodology development, but can not be used for analysis of actual businesses. Generation is described and performed in [this notebook](https://github.com/antonbabkin/rurec/blob/master/nbs/synig.ipynb).


## License

Project code is licensed under the [MIT license](LICENSE.md).

The content and provided data are licensed under the [Creative Commons Attribution 4.0 International license](https://creativecommons.org/licenses/by/4.0/).

