import sys, os

"""Script to update celery config file with the host data.
Needed because server address will be different each time due
to launching ad-hoc EC2 nodes."""

celery_config_file_name = r'celeryconfig.py'
#Don't edit this!
celery_config_file = os.path.join(os.path.split(os.path.abspath(__file__))[0], celery_config_file_name)

if __name__ == "__main__":

	user = sys.argv[1]
	password = sys.argv[2]
	host = sys.argv[3]
	vhost = sys.argv[4]

	with open(celery_config_file, 'rb') as infile:
		contents = infile.readlines()

	with open(celery_config_file, 'wb') as of:
		for line in contents:
			if line.split('=')[0].strip() != 'BROKER_URL':
				of.write(line)
			else:
				out_line = "BROKER_URL = 'amqp://%s:%s@%s:5672/%s'\n" % (user, password, host, vhost)
				of.write(out_line)
