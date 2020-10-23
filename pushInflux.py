import subprocess
import requests
import json
import os
from ConfigParser import SafeConfigParser
import pdb

def query_hadoop(hostname, list_attr):
    Error=None
    trimmed_dict = {}
    command=['curl', '-s', 'http://'+hostname+':50075/jmx?qry=Hadoop:service=DataNode,name=DataNodeActivity-'+hostname+'-50010']

    try:
        fs = subprocess.Popen(command,stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        stdout, stderr = fs.communicate()
    except:
        Error = "Curl command failed"

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

def query_uptime(hostname):
    Error=None
    uptime = None

    command=['curl', '-s', 'http://'+hostname+':50075/jmx?qry=java.lang:type=Runtime']

    try:
        fs = subprocess.Popen(command,stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        stdout, stderr = fs.communicate()
    except:
        Error = "Curl command failed"

    if fs.returncode == 0:
        try:
            bean_dict = json.loads(stdout)
            attr_dict = bean_dict['beans'][0]
        except:
            Error = "failed converting to json"
        else:
            uptime = attr_dict['Uptime']
    else:
        Error = "curl ExitCode != 0"

    return Error, uptime


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
        error_uptime, uptime = query_uptime(hostname)
        if error or error_uptime:
            print("ERROR in : "+hostname+": "+error)
        else:
            uptime_days = uptime/(1000*3600*24)
            data_dict['Uptime']=uptime
            data_dict['DatanodeNetworkErrors_perDay']=float(data_dict['DatanodeNetworkErrors'])/uptime_days
            data_dict['BytesWritten_perDay']=float(data_dict['BytesWritten'])/uptime_days
            data_dict['TotalWriteTime_perDay']=float(data_dict['TotalWriteTime'])/uptime_days
            data_dict['VolumeFailures_perDay']=float(data_dict['VolumeFailures'])/uptime_days

            data = format_for_influx(data_dict)
        r = requests.post(url, auth=(username, password), data=data, timeout=40)

if __name__ == "__main__":
    main()
