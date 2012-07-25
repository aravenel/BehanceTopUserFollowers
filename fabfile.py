import sys
import os
import time
from fabric.api import *
import boto
import boto_config

aws_access_key_id = boto_config.AWS_ACCESS_KEY_ID
aws_secret_access_key = boto_config.AWS_SECRET_ACCESS_KEY
key_pair_location = boto_config.AWS_KEY_PAIR_LOCATION
key_pair_name = boto_config.AWS_KEY_PAIR_NAME

def dev():
    env.user = 'ubuntu'
    env.hosts = get_hosts_by_environment('dev')
    env.key_filename = [os.path.join(key_pair_location, key_pair_name + '.pem')]
    if len(env.hosts) == 0:
        print """No dev hosts provisioned. Do a provision_full_system or provision 
        individual workers via provision_celery_workers or provision_celery_worker_writer.
        Exiting."""
        sys.exit()

def prod():
    env.uset = 'ubuntu'
    env.hosts = get_hosts_by_environment('prod')
    env.key_filename = [os.path.join(key_pair_location, key_pair_name + '.pem')]
    if len(env.hosts) == 0:
        print "No prod hosts provisioned. Exiting."
        sys.exit()

def setup_keypair(ec2_connection, key_pair_name):
    if not os.path.isfile(os.path.join(key_pair_location, key_pair_name + ".pem")):
        try:
            key_pair = ec2_connection.create_key_pair(key_pair_name)
            key_pair.save(key_pair_location)
        #except boto.Exception.EC2ResponseError, e:
        except Exception, e:
            print "Unable to get keys: %s" % e
            sys.exit()
    return True

def get_hosts_by_environment(environment='dev'):
    """Return list of public dns entries of all instances with specified
    environment tag value."""
    ec2 = boto.connect_ec2(aws_access_key_id, aws_secret_access_key)
    reservations = ec2.get_all_instances()
    instances = [i for r in reservations for i in r.instances]
    filtered_instances = [i.public_dns_name for i in instances if 
            i.tags.get('environment') == environment and i.state == 'running']
    return filtered_instances

def provision_celery_workers(
        n=1,
        key_pair_name=key_pair_name,
        key_pair_location=key_pair_location,
        image_id='ami-82fa58eb',
        instance_type='t1.micro',
        placement='us-east-1a',
        security_groups = ['default'],
        environment='dev',
        ):
    """Use boto to provision n number of celery workers"""
    #Create connection object
    print "Connecting to EC2...",
    ec2 = boto.connect_ec2(aws_access_key_id, aws_secret_access_key)
    print "Done."
    #Create keys if needed
    print "Getting and saving keypair...",
    setup_keypair(ec2, key_pair_name)
    print "Done."
    #Reserve instances
    print "Reserving instances...",
    reservation = ec2.run_instances(
        image_id = image_id,
        min_count = n,
        max_count = n,
        key_name = key_pair_name,
        instance_type = instance_type,
        placement = placement,
        security_groups = security_groups,
            )
    print "Done."

    #Wait for them to start and tag them
    for instance_num, instance in enumerate(reservation.instances):
        print "Tagging instance %s..." % instance_num
        status = instance.update()
        while status == 'pending':
            print "\tInstance not ready, sleeping for 10 seconds..."
            time.sleep(10)
            status = instance.update()
        if status == 'running':
            instance.add_tag('environment', environment)
            instance.add_tag('type', 'celery_worker')
        else:
            print "Something is wrong with instance %s. Returned status of %s." % (instance_num, status)
            continue

def deprovision_celery_workers(environment='dev'):
    pass

def provision_celery_worker_writer(n, environment='dev'):
    """Use boto to provision n number of celery csv writer workers"""
    pass

def provision_rabbitmq_server(environment='dev'):
    pass

def provision_full_system(n_workers, n_writers=1, environment='dev'):
    """Provision a full setup using boto on AWS. Requires inputs:
        n_workers: number of celery workers (twitter api consumers)
        n_writers: number of csv writers. Likely should be one.
        environment: dev or prod. Controls tagging of servers.
        """

def deprovision_full_system(environment='dev'):
    pass

def get_aws_hosts(environment):
    """Return list of the hosts from AWS for given environment."""
    pass

def deploy_celery_workers():
    """Deploy code to celery workers"""
    run('sudo apt-get update && sudo apt-get upgrade -y')
    run('sudo apt-get install -y python python-dev python-pip')

def deploy_celery_worker_writers():
    """Deploy code to celery csv writer workers"""
    pass
