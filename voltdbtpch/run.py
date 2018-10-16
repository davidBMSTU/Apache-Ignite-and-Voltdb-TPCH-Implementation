from base import *

if __name__ == "__main__":
	conf = voltdbtpchconf('config.conf')

	if invalid_conf(conf):
		print("Invalid configuration")
		exit(1)
		
	conn = connection(conf)
	runner = voltdbtpchrunner(conf, conn)
		
	start_time = time.time()
	runner.run_queries()
	print("Total execution time = ", (time.time() - start_time), "seconds")

	conn.close()
