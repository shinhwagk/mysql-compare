# Remove-Item -Path dist -Recurse
# python setup.py bdist_wheel
# python -m twine upload dist\*

python3 -m pip install --upgrade pip
python3 -m pip install --upgrade build
python3 -m pip install --upgrade twine

python -m build
# python3 -m twine upload --repository mysql-compare dist/*
python -m twine upload dist/*

pip install mysql-compare==0.0.13

ls *.err.log | while read line; do cat $line; echo ""; echo $line; done