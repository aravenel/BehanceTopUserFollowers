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

code_dir = r'~/code/behance'
git_repo_location = r''

def dev():
    env.user = 'ubuntu'
    env.hosts = _get_hosts_by_environment('dev')
    env.key_filename = [os.path.join(key_pair_location, key_pair_name + '.pem')]
    if len(env.hosts) == 0:
        print """No dev hosts provisioned. Do a provision_full_system or provision 
        individual workers via provision_celery_workers or provision_celery_worker_writer.
        Exiting."""
        sys.exit()

def prod():
    env.user = 'ubuntu'
    env.hosts = _get_hosts_by_environment('prod')
    env.key_filename = [os.path.join(key_pair_location, key_pair_name + '.pem')]
    if len(env.hosts) == 0:
        print "No prod hosts provisioned. Exiting."
        sys.exit()

def _setup_keypair(ec2_connection, key_pair_name):
    if not os.path.isfile(os.path.join(key_pair_location, key_pair_name + ".pem")):
        try:
            key_pair = ec2_connection.create_key_pair(key_pair_name)
            key_pair.save(key_pair_location)
        #except boto.Exception.EC2ResponseError, e:
        except Exception, e:
            print "Unable to get keys: %s" % e
            sys.exit()
    return True

def _get_hosts_by_environment(environment='dev'):
    """Return list of public dns entries of all instances with specified
    environment tag value."""
    ec2 = boto.connect_ec2(aws_access_key_id, aws_secret_access_key)
    reservations = ec2.get_all_instances()
    instances = [i for r in reservations for i in r.instances]
    filtered_instances = [i.public_dns_name for i in instances if 
            i.tags.get('environment') == environment and i.state == 'running']
    return filtered_instances

def provision_ec2_instances(
        type_tag='celery_worker',
        n=1,
        key_pair_name=key_pair_name,
        key_pair_location=key_pair_location,
        image_id='ami-82fa58eb',
        instance_type='t1.micro',
        placement='us-east-1a',
        security_groups = ['default'],
        environment='dev',
        ):
    """Use boto to provision n number of instances"""
    #Create connection object
    print "Connecting to EC2...",
    ec2 = boto.connect_ec2(aws_access_key_id, aws_secret_access_key)
    print "Done."
    #Create keys if needed
    print "Getting and saving keypair...",
    _setup_keypair(ec2, key_pair_name)
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
        print "Tagging instance %s of %s..." % (instance_num + 1, n)
        total_wait_time = 0
        status = instance.update()
        while status == 'pending' and total_wait_time < 180:
            print "\tInstance not ready, sleeping for 10 seconds..."
            time.sleep(10)
            total_wait_time += 10
            status = instance.update()
        if status == 'running':
            instance.add_tag('environment', environment)
            instance.add_tag('type', type_tag)
        else:
            if total_wait_time >= 180:
                print "After 3 minutes, instance %s still not provisioned. Something wrong with it?" % instance.id
                continue
            else:
                print "Something is wrong with instance %s. Returned status of %s." % (instance.id, status)
                continue

def deprovision_ec2_instances(type_tag, environment='dev'):
    ec2 = boto.connect_ec2(aws_access_key_id, aws_secret_access_key)
    reservations = ec2.get_all_instances()
    instances = [i for r in reservations for i in r.instances]
    to_terminate = [i.id for i in instances if 
            i.tags.get('environment') == environment and i.state == 'running'
            and i.tags.get('type') == type_tag]
    if len(to_terminate) > 0:
        print "Terminating %s instances..." % len(to_terminate),
        ec2.terminate_instances(to_terminate)
    else:
        "No matching servers to terminate."
    remaining = [i for r in reservations for i in r.instances if i.state=='running']
    print "Done. %s instances remain running." % len(remaining)

def provision_celery_workers(n, environment='dev', instance_type='t1.micro'):
    """Use boto to provision n number of celery workers"""
    provision_ec2_instances(type_tag="celery_worker", n=n, 
            instance_type=instance_type, environment=environment)

def deprovision_celery_workers(environment='dev'):
    """Use boto to deprovision (terminate) n number of celery workers"""
    deprovision_ec2_instances(type_tag='celery_worker', environment=environment)

def provision_celery_writer(n=1, environment='dev', instance_type='t1.micro'):
    """Use boto to provision n number of celery csv writer workers"""
    provision_ec2_instances(type_tag='celery_writer', n=n, 
            instance_type=instance_type, environment=environment)

def deprovision_celery_writer(environment='dev'):
    """Use boto to deprovision (terminate) n number of celery csv writer workers"""
    deprovision_ec2_instances(type_tag='celery_writer', environment=environment)

def provision_rabbitmq_server(environment='dev', instance_type='m1.small'):
    """Use boto to provision rabbitmq server"""
    provision_ec2_instances(type_tag='rabbitmq', instance_type=instance_type,
            environment=environment)

def deploy_celery():
    """Deploy base code to celery workers (both base workers and writers.) 
    Installs all packages as these are shared, but does not configure."""
    run('sudo apt-get update && sudo apt-get upgrade -y')
    run('sudo apt-get install -y python python-dev python-pip git-core')
    run('sudo pip install virtualenv')
    run('mkdir -p %s' % code_dir)
    run('cd %s' % code_dir)
    run('virtualenv venv')
    run('source ./venv/bin/activate')
    run('sudo pip install requests celery')

def deploy_celery_workers(environment):

def config_celery_workers():
    """Deploy and configure code for base celery workers."""
    #Do base-worker specific configuration
    pass

def config_celery_writers():
    """Deploy and configure code for celery writer workers."""
    #Do writer worker specific configuration
    pass

def deploy_rabbitmq():
    """Install required rabbitmq packages."""
    run('sudo apt-get update && sudo apt-get upgrade -y')
    run('sudo apt-get install rabbitmq-server')

def config_rabbitmq():
    """Configure rabbitmq packages."""
    run('sudo rabbitmqctl add_user ravenel sailor')
    run('sudo rabbitmqctl add_vhost myvhost')
    run('sudo rabbitmqctl set_permissions -p myvhost ravenel ".*" ".*" ".*"')

def provision_full_system(n_workers, n_writers=1, environment='dev'):
    """Provision a full setup using boto on AWS. Requires inputs:
        n_workers: number of celery workers (twitter api consumers)
        n_writers: number of csv writers. Likely should be one.
        environment: dev or prod. Controls tagging of servers.
        """
    print "Provisioning celery workers..."
    provision_celery_workers(n_workers, environment=environment)
    print "Provisioning celery writers..."
    provision_celery_writer(n_writers, environment=environment)
    print "Provisioning rabbitmq server..."
    provision_rabbitmq_server(environment=environment)
    with settings(parallel=True, host_string=_get_hosts_by_environment(environment)):
        print "Configuring celery workers..."


        print "Configuring celery writer..."
        print "Configuring rabbitmq server..."
        print "Deploying code to servers..."

def deprovision_full_system(environment='dev'):
    deprovision_ec2_instances(type_tag='celery_worker', environment=environment)
    deprovision_ec2_instances(type_tag='celery_writer', environment=environment)
    deprovision_ec2_instances(type_tag='rabbitmq', environment=environment)

def push():
    pass

def pull():
    pass

def pushpull():
    pass
