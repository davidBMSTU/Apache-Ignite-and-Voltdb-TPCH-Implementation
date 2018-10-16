import configparser
import jaydebeapi
import sys
import json
import time
import signal
import datetime
import fileinput
from additional_functions import *


#Convert any unknown types to string in json.dumps
class Stringifier(json.JSONEncoder):
	def default(self, obj):
		return str(obj)


#Сontain the necessary paths and arguments of the benchmark
class aitpchconf:
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
		self.conf_dict.update({'distributed_joins' : config['BENCHMARK-CONF']['distributed_joins']})
		self.conf_dict.update({'CREATE_INDEXES' : config['BENCHMARK-CONF']['CREATE_INDEXES']})
		self.conf_dict.update({'gen_data' : config['BENCHMARK-CONF']['gen_data']})

	def get(self, key):
		return self.conf_dict[key]

	def set(self, key, value):
		self.conf_dict[key] = value


class connection:
	def __init__(self, config):
		assert (isinstance(config, aitpchconf))

		driver_name = "org.apache.ignite.IgniteJdbcThinDriver"
		lib = config.get('ignite_core_lib')
		ip = config.get('IP')
		distributed_joins = config.get('distributed_joins')
		url = "jdbc:ignite:thin://" + ip

		if distributed_joins == "True":
			url = url + ";distributedJoins=true"
			print("Connecting to Apache Ignite cluster at " + ip + ", distributed joins ON...")
		else:
			print("Connecting to Apache Ignite cluster at " + ip + ", distributed joins OFF...")

		try:
			self.conn = jaydebeapi.connect(driver_name, url, [] , lib, lib)
		except Exception as e:
			print("Connecting to cluster failed")
			print("The error is:\n\n" + str(e))
			exit()
			
		print("Connection complite")


	def get_cursor(self):
		return self.conn.cursor()


	def close(self):
		self.conn.close()


class aitpchrunner:
	def __init__(self, config, ai_conn):
		assert (isinstance(config, aitpchconf))
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
		table_path = self.conf.get('tpch_data')
		
		try:
			print("Dropping tables if exists...")
			run_from_file("drop", cur)
			print("Dropping tables complite")
		except:
			print("No tables\n")

		print("Creating tables...")
		run_from_file("dss.ddl", cur)
		print("Creating tables complite")

		if self.conf.get('CREATE_INDEXES') == "True":
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

		tbls_names = ["/nation.tbl", "/customer.tbl", "/region.tbl",\
		"/lineitem.tbl", "/orders.tbl", "/part.tbl", "/partsupp.tbl",\
		 "/supplier.tbl"]

		print("Conversation to CSV - START...")

		for i in range(len(tbls_names)):
				with fileinput.FileInput(table_path + tbls_names[i], inplace=True) as file:
						for line in file:
								print(line.replace(",", ""), end='')

		for i in range(len(tbls_names)):
				with fileinput.FileInput(table_path + tbls_names[i], inplace=True) as file:
						for line in file:
								print(line.replace("|", ","), end='')
				print(tbls_names[i][1:] + " - complite")

		print("Conversation to CSV - OK")


		print("Start putting data...\n")
		
		intos = ["' INTO NATION (N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT) FORMAT CSV",\
		"' INTO CUSTOMER (C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT) FORMAT CSV",
		"' INTO REGION (R_REGIONKEY, R_NAME, R_COMMENT) FORMAT CSV",\
		"' INTO LINEITEM (L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT)  FORMAT CSV",\
		"' INTO ORDERS (O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT) FORMAT CSV",\
		"' INTO PART (P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT) FORMAT CSV",\
		"' INTO PARTSUPP (PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT) FORMAT CSV",\
		"' INTO SUPPLIER (S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT) FORMAT CSV"] 

		for i in range(len(tbls_names)):
			file_path = table_path + tbls_names[i]
			q =  "COPY FROM '" + file_path + intos[i]
			print("Putting " + tbls_names[i][1:] + " table...")
			
			try:
				cur.execute(q)
			except Exception as e:
				print("Something wrong with putting data.")
				print("The error is:\n\n" + str(e))
				exit()

			print("Putting " + tbls_names[i][1:] + " table complite")

		print("Putting data complite")

		cur.close()


	def run_queries(self):
		cur = self.conn.get_cursor()

		numruns = self.conf.get('numruns')
		tpch_queries = self.conf.get('tpch_queries')

		query_nums = [1, 2, 3, 4, 6, 7, 8, 9, 10, 12, 14, 17, 21, 22]
		#query_nums += [2, 16, 20, 22] #slow
		#queries 11, 13, 15 - contains not-supported elements in Apache Ignite

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
				query_time_msg = str(query_nums[i]) + " query execution time = " + \
				str(query_time) + " seconds"
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
