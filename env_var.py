import os

os.environ['envn'] = 'DEV'
os.environ['header'] = 'True'
os.environ['InferSchema'] = 'True'

header = os.environ['header']
InferSchema = os.environ['InferSchema']
envn = os.environ['envn']

# App_Name for Spark Enviroment/Session...
appName = 'firstproject'

# To get the currect working directory...
act_path = os.getcwd()

# Creating path to the source files...
src_path = act_path + '\\Source'