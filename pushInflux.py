import subprocess
import requests
import json
import os
from ConfigParser import SafeConfigParser

def query_hadoop(hostname, list_attr):
	Error=None
	trimmed_dict = {}
	
	command=['curl', '-s', 'http://'+hostname+':50075/jmx?qry=Hadoop:service=DataNode,name=DataNodeActivity-'+hostname+'-50010']
	
	try:		
		fs = subprocess.Popen(command,stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
		stdout, stderr = fs.communicate()
	except:
		Error = "failed converting to json"
	
	if fs.returncode == 0:
		try:	
			bean_dict = json.loads(stdout)
			attr_dict = bean_dict['beans'][0]
		except:
			Error = "failed converting to json"
		else:	
			for key in attr_dict:
				if key in list_attr:
					trimmed_dict[key]=attr_dict[key]
	else:
		Error = "curl ExitCode != 0"
	
	return Error, trimmed_dict


def format_for_influx(trimmed_dict):
	data = ""
	hostname = trimmed_dict['tag.Hostname']
	for key in trimmed_dict:
		if key != 'tag.Hostname':
			data+=key+",host="+hostname+" value="+str(trimmed_dict[key])+"\n"
	return data
	
def main():
	url = 'http://graph.t2.ucsd.edu:8086/write?db=hadoop_performance_metrics_db'

	base_dir = os.path.dirname(__file__)
	if base_dir != "":
		base_dir+="/"
	print("base_dir: "+base_dir)
	# Read from config file
	parser = SafeConfigParser()
	try:
		parser.read(base_dir+'conf')
		username = parser.get('auth', 'username')
		password = parser.get('auth', 'password')
	except:
		raise NameError('Unable to read from config file: '+base_dir+'conf')	

	
	list_attr = []
	fd = open(base_dir+'list_attributes')
	lines = fd.readlines()
	for attr in lines:
		list_attr.append(attr[:-1])
	
	list_nodes= []
	fd = open(base_dir+'list_nodes')
	lines_nodes = fd.readlines()
	for node in lines_nodes:
		list_nodes.append(node[:-1])
	
	for hostname in list_nodes:
		error, data_dict = query_hadoop(hostname, list_attr)
		if error:
			print("ERROR in : "+hostname+": "+error)
		else:
			data = format_for_influx(data_dict)
		print(data)
		#r = requests.post(url, auth=(username, password), data=data, timeout=40)

if __name__ == "__main__":
	main()
