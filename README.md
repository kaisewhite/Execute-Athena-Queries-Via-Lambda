# Execute-Athena-Queries-Via-Lambda

How to execute athena queries from a lambda function and dump the results to an s3 bucket

### How to Run

1. Open a new terminal
2. Set the AWS Profile you want to run the script as:

```
export AWS_DEFAULT_PROFILE=<AWS_PROFILE>
```

3. Run the following command in the terminal:

```
python3 app.py
```

4. Or if you want a one liner:

```
export AWS_DEFAULT_PROFILE=<AWS PROFILE NAME> && python3 app.py
```
