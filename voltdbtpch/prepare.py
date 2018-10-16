from base import *

if __name__ == "__main__":
	conf = voltdbtpchconf('config.conf')

	if invalid_conf(conf):
		print("VOLTTPCH - Invalid configuration")
		exit(1)
		
	conn = connection(conf)
	runner = voltdbtpchrunner(conf, conn)

	if conf.get('gen_data') == "True":
		start_time = time.time()
		runner.gen_data_queries()
		print("Data gen time = ", (time.time() - start_time), "seconds")

		
	start_time = time.time()
	runner.put_data()
	print("Сreating and populating tables time = ", (time.time() - start_time), "seconds")

	if conf.get('CREATE_INDEXES') == "True":
		start_time = time.time()
		runner.create_indexes()
		print("Сreating indexes time = ", (time.time() - start_time), "seconds")
	
	conn.close()