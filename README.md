# 100k_moka_booking_in v1.0

This script parses the ouput from [100k_negnegs/negneg_cases.py](https://github.com/moka-guys/100k_negnegs) and creates NGStest requests in Moka for all cases (not just negnegs) that are not already in Moka.

The script outputs a tab seperated logfile, showing the action performed on each case, and flagging cases that couldn't be entered to Moka and the reason why.


### Usage

This script requires access to Moka via ODBC.

Requirements:
    ODBC connection to Moka
    Python 3.6
    pyodbc

On `SV-PR-GENAPP01` activate the `100k_moka_booking_in` conda environment so that above requirements are met:

```
conda activate `100k_moka_booking_in`
```

```
python 100k2moka.py -i INPUT_FILE -o OUTPUT_FILE
```
