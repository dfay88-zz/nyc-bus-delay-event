# nyc-bus-delay-event
NYC bus delay in response to major events

## Abstract

## Reproducing Our Research

### 1. Download the data
To download the raw, external data, first run in the root directory of this project:

```
make download_data
```

This will create a collection of directories under `data/` that will store all
external. This may take a few minutes depending on the
speed of your internet.

### 2. Process data

Run through all notebooks in the `code` folder in the order of their prefix.
This will generate data in the `/data/processed` folders to be used for analysis.

>Naming convention for all notebooks in the project: order number, the creator's initials, and a short `-` delimited description, e.g. `1.0-jp-initial-data-exploration`.

### 3. Analyze Data

Run through all notebooks in the `/code/models` folder. These notebooks will run various regressions, outputting
our findings.

## Project Organization

    ├── Makefile           <- Makefile with commands like `make download_data`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   └── processed      <- The final, canonical data sets for modeling.
    │   
    ├── code               
    │   ├── process        <- Data processing Jupyter notebooks.
    │   └── models         <- Models and analysis Jupyter notebooks.
    |
    └── references         <- Data dictionaries, manuals, and all other explanatory materials.

## Data Sources

| Data                       | Source                                                                                                             |
|:---------------------------|:-------------------------------------------------------------------------------------------------------------------|
| NYC Bus Data | [MTA Historical Bus Time Data](http://data.mytransit.nyc/bus_time/)                                                   |
