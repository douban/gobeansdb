#!/bin/bash

virtualenv venv
source venv/bin/activate
venv/bin/pip install -r tests/pip-req.txt
venv/bin/nosetests --with-xunit --xunit-file=unittest.xml
deactivate