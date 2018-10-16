from base import *

if __name__ == "__main__":
	conf = aitpchconf('config.conf')

	if invalid_conf(conf):
		print("AITPCH - Invalid configuration")
		exit(1)
		
	conn = connection(conf)
	runner = aitpchrunner(conf, conn)

	if conf.get('gen_data') == "True":
		start_time = time.time()
		runner.gen_data_queries()
		print("Data gen time = ", (time.time() - start_time), "seconds")
	
	start_time = time.time()
	runner.put_data()
	print("AITPCH - Сreating and populating tables time = ", (time.time() - start_time), "seconds")
	
	conn.close()