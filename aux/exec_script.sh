#!/bin/sh

source /opt/miniconda3/bin/activate isos_py_env

python aux/isos_exec_script.py ${@:1}

