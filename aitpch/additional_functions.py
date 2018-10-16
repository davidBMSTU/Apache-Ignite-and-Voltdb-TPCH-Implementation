import os

def invalid_conf(conf):
	if conf.get('scale') <= 0 or conf.get('numruns') <= 0:
		print("AITPCH - Invalid values in configuration file: scale or numruns")
		return True

	if os.path.isdir(conf.get('tpch_data')) == False:
		if os.path.isdir('tpch_data') == False:
			os.mkdir('tpch_data')
		conf.set('tpch_data', 'tpch_data')

	if os.path.isdir(conf.get('tpch_queries')) == False:
		if os.path.isdir('tpch_queries') == False:
			os.mkdir('tpch_queries')
		conf.set('tpch_queries', 'tpch_queries')

	if os.path.isdir(conf.get('res')) == False:
		if os.path.isdir('res') == False:
			os.mkdir('res')
		conf.set('res', 'res')

	return False


def run_from_file(filename, cur):
	file_str = ""
	with open(filename, "r") as f:
		file_str = f.read()
	
	ind = 0
	for i in range(len(file_str)):
		if file_str[i] == ';':
			cur.execute(file_str[ind:i])
			ind = i + 1

