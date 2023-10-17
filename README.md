# Multi-GNN
Multi-GNN architectures for Anti-Money Laundering. This repository contains all models and adaptations needed to run Multi-GNN for Anti-Money Laundering.

## Setup
To use the repository, you first need to install the conda environment via 
```
conda env create -f env.yml
```
Then, the data needed for the experiments can be found on [Kaggle](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml/data). Make sure to change the filepath at the beginning of the `data_loading.py` file to wherever you stored the data on your local machine.

## Usage
To run the experiments you need to run the `main.py` function and specify any arguments you want to use. There are two required arguments, namely `--data` and `--model`. For the `--data` argument, make sure you store the different datasets in different folders. Then, specify the folder name, e.g `--data Small_HI`. The `--model` parameter should be set to any of the model classed that are available, i.e. to one of `--model [gin, gat, rgcn, pna]`. Thus, to run a standard GNN, you need to run, e.g.:
```
python main.py --data Small_HI --model gin
```
Then you can add different adaptations to the models by selecting the respective arguments from:

<div align="center">

| Column 1       | Column 2                     |
| -------------- | ---------------------------- |
| `--emlps`      | Edge updates via MLPs        |
| `--reverse_mp` | Reverse Message Passing      |
| `--ego`        | Ego ID's to the center nodes |
| `--ports`      | Port Numberings for edges    |

</div>
Thus, to run Multi-GIN, you would run the follwing command:

```
python main.py --data Small_HI --model gin --emlps --reverse_mp --ego --ports
```

## Licence
Apache License
Version 2.0, January 2004