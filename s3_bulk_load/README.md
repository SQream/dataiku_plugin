# Plugin information
 

The purpose of this Plugin is to speed up data transfer from s3 to sqreamd.

How It Works:

Sqreamd and S3 are deeply integrated solutions. If you already have an AWS account and use S3 buckets for storing and managing your data, you can make use of your existing buckets and folder paths for bulk loading into sqreamdb. This DSS Plugin allows you to load the data from S3 to sqreamdb directly, without any external actions - and ensures fast data transfers.


A typical usage scenario would be:

read some input data from Amazon S3 and load it to Sqreamdb table by using this DSS Plugin.
 

The Plugin allows DSS to create a new sqreamdb table and to perform a fast bulk load from S3 data.

Prerequisites
Sqreamdb (JDBC) Connection set up in DSS
Amazon S3 Connection set up in DSS
the corresponding AWS credentials for the S3 buckets (AWS Access Key and AWS Secret Key)
The Plugin comes with a code environment that installs the Sqreamdb Python Connector(pysqream) and is automatically installed with the Plugin. 


The Plugin has been tested with Python 3.9 and requires a valid Python 3.9 installation on the machine (the Plugin code environment is restricted to Python 3.9).

# How To Use
In order to use the Plugin:

Defined a DSS S3 Dataset
Add the Plugin to your Flow
Set the S3 Dataset as Input of the Plugin (mandatory - only S3 is supported)
Assign a name for the output Dataset, stored in your Sqreamd Connection
The Plugin requires 2 input parameters: the AWS Access Key and AWS Secret Key. You can either:

fill in the values in the Plugin form
or set them Project Variables
or set the in Global Variables
When DSS Variables are used, DSS will look for the following inputs:

{
    "aws_access_key": "your-aws-access-key",
    "aws_secret_key": "your-aws-secret-key"
}
Finally, run the Plugin Recipe and browse the output Dataset. A new table should have been created in Sqreamdb.

Error Handling
If the Plugin job fails, you can look at the error logs for the cause of the problem. Depending on the error message, the errors might be due to:

Missing Key_ID and/or Secret_Key: “KeyError: 'AWS_KEY_ID' (or 'AWS_SECRET_KEY)”
Wrong Secret_Key: “SignatureDoesNotMatch”
Wrong Key_ID: “The AWS Access Key Id you provided is not valid”
