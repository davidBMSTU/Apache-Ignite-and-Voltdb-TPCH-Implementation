from base import *

if __name__ == "__main__":
	conf = aitpchconf('config.conf')

	if invalid_conf(conf):
		print("AITPCH - Invalid configuration")
		exit(1)
		
	conn = connection(conf)
	runner = aitpchrunner(conf, conn)

	start_time = time.time()
	runner.run_queries()
	print("AITPCH - Query execution time = ", (time.time() - start_time), "seconds")

	conn.close()
