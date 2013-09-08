
__all__ = ['download_tool', 'get_tool']

from lixian_config import *
import subprocess
import urllib2, base64
import os.path
import json
import time

download_tools = {}

def download_tool(name):
	def register(tool):
		download_tools[name] = tool_adaptor(tool)
		return tool
	return register

class DownloadToolAdaptor:
	def __init__(self, tool, **kwargs):
		self.tool = tool
		self.client = kwargs['client']
		self.url = kwargs['url']
		self.path = kwargs['path']
		self.resuming = kwargs.get('resuming')
		self.size = kwargs['size']
	def finished(self):
		assert os.path.getsize(self.path) <= self.size, 'existing file (%s) bigger than expected (%s)' % (os.path.getsize(self.path), self.size)
		return os.path.getsize(self.path) == self.size
	def __call__(self):
		self.tool(self.client, self.url, self.path, self.resuming)

def tool_adaptor(tool):
	import types
	if type(tool) == types.FunctionType:
		def adaptor(**kwargs):
			return DownloadToolAdaptor(tool, **kwargs)
		return adaptor
	else:
		return tool


def check_bin(bin):
	import distutils.spawn
	assert distutils.spawn.find_executable(bin), "Can't find %s" % bin

@download_tool('urllib2')
def urllib2_download(client, download_url, filename, resuming=False):
	'''In the case you don't even have wget...'''
	assert not resuming
	print 'Downloading', download_url, 'to', filename, '...'
	request = urllib2.Request(download_url, headers={'Cookie': 'gdriveid='+client.get_gdriveid()})
	response = urllib2.urlopen(request)
	import shutil
	with open(filename, 'wb') as output:
		shutil.copyfileobj(response, output)

@download_tool('asyn')
def asyn_download(client, download_url, filename, resuming=False):
	import lixian_download_asyn
	lixian_download_asyn.download(download_url, filename, headers={'Cookie': 'gdriveid='+str(client.get_gdriveid())}, resuming=resuming)

@download_tool('wget')
def wget_download(client, download_url, filename, resuming=False):
	gdriveid = str(client.get_gdriveid())
	wget_opts = ['wget', '--header=Cookie: gdriveid='+gdriveid, download_url, '-O', filename]
	if resuming:
		wget_opts.append('-c')
	wget_opts.extend(get_config('wget-opts', '').split())
	check_bin(wget_opts[0])
	exit_code = subprocess.call(wget_opts)
	if exit_code != 0:
		raise Exception('wget exited abnormally')

@download_tool('curl')
def curl_download(client, download_url, filename, resuming=False):
	gdriveid = str(client.get_gdriveid())
	curl_opts = ['curl', '-L', download_url, '--cookie', 'gdriveid='+gdriveid, '--output', filename]
	if resuming:
		curl_opts += ['--continue-at', '-']
	curl_opts.extend(get_config('curl-opts', '').split())
	check_bin(curl_opts[0])
	exit_code = subprocess.call(curl_opts)
	if exit_code != 0:
		raise Exception('curl exited abnormally')

@download_tool('aria2')
@download_tool('aria2c')
class Aria2DownloadTool:
	def __init__(self, **kwargs):
		self.gdriveid = str(kwargs['client'].get_gdriveid())
		self.url = kwargs['url']
		self.path = kwargs['path']
		self.size = kwargs['size']
		self.resuming = kwargs.get('resuming')
	def finished(self):
		assert os.path.getsize(self.path) <= self.size, 'existing file (%s) bigger than expected (%s)' % (os.path.getsize(self.path), self.size)
		return os.path.getsize(self.path) == self.size and not os.path.exists(self.path + '.aria2')
	def __call__(self):
		gdriveid = self.gdriveid
		download_url = self.url
		path = self.path
		resuming = self.resuming
		dir = os.path.dirname(path)
		filename = os.path.basename(path)
		aria2_opts = ['aria2c', '--header=Cookie: gdriveid='+gdriveid, download_url, '--out', filename, '--file-allocation=none']
		if dir:
			aria2_opts.extend(('--dir', dir))
		if resuming:
			aria2_opts.append('-c')
		aria2_opts.extend(get_config('aria2-opts', '').split())
		check_bin(aria2_opts[0])
		exit_code = subprocess.call(aria2_opts)
		if exit_code != 0:
			raise Exception('aria2c exited abnormally')

@download_tool('aria2rpc')
def aria2rpc_download(client, download_url, path, resuming=False):
	gdriveid = str(client.get_gdriveid())
	dir = os.path.dirname(path)
	filename = os.path.basename(path)
	aria2rpchost = get_config('aria2-rpc-host', 'localhost')
	aria2rpcport = get_config('aria2-rpc-port', '6800')
	aria2rpcuser = get_config('aria2-rpc-user', None)
	aria2rpcpass = get_config('aria2-rpc-pass', '')

	ts = str(int(time.time()*1000))
	data = {"jsonrpc": "2.0",
		"method":"aria2.addUri",
		"id": ts,
		"params": [[download_url],
			{
			"dir": dir,
			"out": filename,
			"header": "Cookie: gdriveid="+gdriveid}
			]
		}
	data = json.dumps(data)

	request_url = "http://"+aria2rpchost+":"+aria2rpcport+"/jsonrpc?tm="+ts

	if aria2rpcuser:
		request = urllib2.Request(request_url)
		base64string = base64.encodestring('%s:%s' % (aria2rpcuser, aria2rpcpass)).replace('\n', '')
		request.add_header("Authorization", "Basic %s" % base64string)
		urllib2.urlopen(request, data)
	else:
		urllib2.urlopen(request_url, data)


@download_tool('axel')
def axel_download(client, download_url, path, resuming=False):
	gdriveid = str(client.get_gdriveid())
	axel_opts = ['axel', '--header=Cookie: gdriveid='+gdriveid, download_url, '--output', path]
	axel_opts.extend(get_config('axel-opts', '').split())
	check_bin(axel_opts[0])
	exit_code = subprocess.call(axel_opts)
	if exit_code != 0:
		raise Exception('axel exited abnormally')

def get_tool(name):
	return download_tools[name]


