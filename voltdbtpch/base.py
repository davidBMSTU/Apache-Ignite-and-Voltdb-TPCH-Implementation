import configparser
import jaydebeapi
import sys
import json
import time
import datetime
from additional_functions import *

#Convert any unknown types to string in json.dumps
class Stringifier(json.JSONEncoder):
	def default(self, obj):
		return str(obj)


#Сontain the necessary paths and arguments of the benchmark
class voltdbtpchconf:
	def __init__(self, filename):
		config = configparser.ConfigParser()
		config.read('config.conf')

		self.conf_dict = {}

		self.conf_dict.update({'dbgenpath' : config['PATHS']['dbgenpath']})
		self.conf_dict.update({'tpch_data' : config['PATHS']['tpch_data']})
		self.conf_dict.update({'tpch_queries' : config['PATHS']['tpch_queries']})
		self.conf_dict.update({'ignite_core_lib' : config['PATHS']['ignite_core_lib']})
		self.conf_dict.update({'res' : config['PATHS']['res']})

		self.conf_dict.update({'scale' : int(config['BENCHMARK-CONF']['scale'])})
		self.conf_dict.update({'numruns' : int(config['BENCHMARK-CONF']['numruns'])})
		self.conf_dict.update({'IP' : config['BENCHMARK-CONF']['IP']})
		self.conf_dict.update({'partition' : config['BENCHMARK-CONF']['partition']})
		self.conf_dict.update({'CREATE_INDEXES' : config['BENCHMARK-CONF']['CREATE_INDEXES']})
		self.conf_dict.update({'gen_data' : config['BENCHMARK-CONF']['gen_data']})

	def get(self, key):
		return self.conf_dict[key]

	def set(self, key, value):
		self.conf_dict[key] = value


class connection:
	def __init__(self, config):
		assert (isinstance(config, voltdbtpchconf))
		driver_name = "org.voltdb.jdbc.Driver"
		lib = config.get('ignite_core_lib')
		ip = config.get('IP')
		url = "jdbc:voltdb://" + ip + ":21212"

		print("Connecting to Voltdb cluster at " + ip + "...")
		
		try:
			self.conn = jaydebeapi.connect(driver_name, url, [], lib, lib)
		except Exception as e:
			print("Connecting to cluster failed")
			print("The error is:\n\n" + str(e))
			exit()

		print("Connection complite")


	def get_cursor(self):
		return self.conn.cursor()


	def close(self):
		self.conn.close()


class voltdbtpchrunner:
	def __init__(self, config, ai_conn):
		assert (isinstance(config, voltdbtpchconf))
		assert (isinstance(ai_conn, connection))
		self.conf = config
		self.conn = ai_conn


	def gen_data_queries(self):
		dbgenpath = self.conf.get('dbgenpath')
		tpch_data = self.conf.get('tpch_data')
		tpch_queries = self.conf.get('tpch_queries')
		actual_path = os.getcwd()
		
		#USING dbgen for generating data for data base, that will be put in Apache Ignite

		# cp dbgens/dbgen-typed/dists.dss tpch_data/dists.dss
		# needed for dbgen
		print("Start generating data...\n")
		try:
			comm = "cp " + dbgenpath + "/dists.dss " + tpch_data + "/dists.dss"
			os.system(comm)

			# cp dbgens/dbgen-typed/dss.ddl tpch_data/dss.ddl
			# needed in data putting process
			comm = "cp " + dbgenpath + "/dss.ddl " + tpch_data + "/dss.ddl" 
			os.system(comm)

			# cp dbgens/dbgen-typed/dss.ri tpch_data/dss.ri
			# needed in data putting process
			comm = "cp " + dbgenpath + "/dss.ri " + tpch_data + "/dss.ri" 
			os.system(comm)

			# generating data in tpch_data
			os.chdir(tpch_data)
			comm = "../" + dbgenpath + "/./dbgen -f -s " + str(self.conf.get('scale'))
			os.system(comm)
			os.chdir(actual_path)

		except Exception as e:
			print("It can be something wrong with benchmark paths. Check configuration file")
			print("The error is:\n\n" + str(e))
			exit()
		
		print("Generating data complite\n")

		'''
		#NOT RELEVANT, every query must be rewritten for correct working with Apache Ignite
		#because of unsupported operations, types, ... 

		#Using qgen for generating queries


		# cp dbgens/dbgen-typed/dists.dss tpch_queries/dists.dss
		# needed for qgen
		comm = "cp " + dbgenpath + "/dists.dss " + tpch_queries + "/dists.dss" 
		os.system(comm)

		# generating queries into tpch_queries
		os.chdir("tpch_test")
		for i in range(1, 23):
			str_i = str(i)
			# *.sql, needed by qgen
			comm = "cp " + "../" + dbgenpath + "/queries/" + str_i + ".sql " + \
					"./" + str_i + ".sql"
			os.system(comm)

			# qgen ...
			comm = "../" + dbgenpath + "/./qgen " + str_i + " > " + "q0" + str_i + ".sql"
			os.system(comm)

			# rm *.sql files
			comm = "rm " + str_i + ".sql"
			os.system(comm)

		os.chdir(actual_path)
		'''


	def put_data(self):
		cur = self.conn.get_cursor()
		tpch_data = self.conf.get('tpch_data')
		partition = self.conf.get('partition')
		ip = self.conf.get('IP')
		actual_path = os.getcwd()

		try:
			print("Dropping tables if exists...")
			run_from_file("drop", cur)
			print("Dropping tables complite")
		except:
			print("No tables\n")

		print("Creating tables...")
		run_from_file("dss.ddl", cur)
		print("Creating tables complite")

		if partition == "True":
			print("Partitioning tables...")
			
			run_from_file("partition", cur)

			print("Partitioning tables complite")


		tbls_names = ["nation", "customer", "region",\
		"lineitem", "orders", "part", "partsupp",\
		 "supplier"]

		os.chdir("volt_temp_logs")

		print("Start putting data...")

		tpch_data += "/"
		for i in range(len(tbls_names)):
			comm = 'csvloader --separator "|" --servers ' + ip + ' --file ' + "../" + tpch_data + tbls_names[i] \
			 + ".tbl " + tbls_names[i] + ' > temp_logs'
			print("Putting " + tbls_names[i] + " table...")

			try:
				os.system(comm)
			except Exception as e:
				print("Something wrong with putting data.")
				print("The error is:\n\n" + str(e))
				exit()

			print("Putting " + tbls_names[i] + " table complite")

		print("Putting data complite")

		os.chdir(actual_path)
		cur.close()

	def create_indexes(self):
		cur = self.conn.get_cursor()
		
		print("Start creating indexes")
			
		cur.execute("CREATE INDEX i_n_regionkey ON nation (n_regionkey);")
		cur.execute("CREATE INDEX i_s_nationkey ON supplier (s_nationkey);")
		cur.execute("CREATE INDEX i_c_nationkey ON customer (c_nationkey);")
		cur.execute("CREATE INDEX i_ps_suppkey ON partsupp (ps_suppkey);")
		cur.execute("CREATE INDEX i_ps_partkey ON partsupp (ps_partkey);")
		cur.execute("CREATE INDEX i_o_custkey ON orders (o_custkey);")
		cur.execute("CREATE INDEX i_l_orderkey ON lineitem (l_orderkey);")
		cur.execute("CREATE INDEX i_l_suppkey_partkey ON lineitem (l_partkey, l_suppkey);")
		

		print("Creating indexes complite")

		cur.close()


	def drop_indexes(self):
		cur = self.conn.get_cursor()

		print("Start dropping indexes")

		cur.execute("DROP INDEX i_n_regionkey;")
		cur.execute("DROP INDEX i_s_nationkey;")
		cur.execute("DROP INDEX i_c_nationkey;")
		cur.execute("DROP INDEX i_ps_suppkey;")
		cur.execute("DROP INDEX i_ps_partkey;")
		cur.execute("DROP INDEX i_o_custkey;")
		cur.execute("DROP INDEX i_l_orderkey;")
		cur.execute("DROP INDEX i_l_suppkey_partkey;")


		print("Dropping indexes complite")
		
		cur.close()


	def run_queries(self):
		cur = self.conn.get_cursor()
		numruns = self.conf.get('numruns')
		tpch_queries = self.conf.get('tpch_queries')

		query_nums = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]
		#queries 2, 16, 20, 22 slow
		#queries 11, 15 - contains not-supported elements in Voltdb

		result_file_name = '{0:%Y-%m-%d_%H:%M:%S}'.format(datetime.datetime.now())
		res_dir = self.conf.get('res')
		final_path = res_dir + "/" + result_file_name
		os.mkdir(final_path)
		time_list_file_path = final_path + "/time.txt"

		for i in range(len(query_nums)):
			constant = tpch_queries + "/q0" + str(query_nums[i]) +".sql"

			with open(constant, "r") as q_f: #getting query from q0*.sql
				while q_f.read(1) != '\n':
					pass
				query = q_f.read().replace('\n', ' ').replace('\t', ' ')
			
			try:
				total_time = 0

				print("\nQuery " + str(query_nums[i]) + " is executing...")
				for j in range(numruns):
					start_time = time.time()
					cur.execute(query)
					total_time += time.time() - start_time

				query_time = round(total_time/numruns, 4)
				query_time_msg = str(query_nums[i]) + " query execution time = " \
				+ str(query_time) + " seconds"
				print(query_time_msg)

				answer = cur.fetchall()
				answer_json = json.dumps(answer, indent=4, cls=Stringifier)

				res_file_path = final_path + "/" + str(query_nums[i]) + ".out"
				with open(res_file_path, "w") as f:
					f.write(query_time_msg + "\n" + answer_json)
				with open(time_list_file_path, "a") as time_list:
					time_list.write(query_time_msg + "\n")
				
			except Exception as e:
				error_msg = str(query_nums[i]) + " query ends with an error. Look to the res dir for more info."
				print(error_msg)

				with open(time_list_file_path, "a") as time_list:
					time_list.write(str(query_nums[i]) + " query: error\n")

				res_file_path = final_path + "/" + str(query_nums[i]) + ".out"
				with open(res_file_path, "w") as f:
					f.write(str(e))
