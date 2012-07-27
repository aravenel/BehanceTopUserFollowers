import sys
import os
import time
from fabric.api import *
import boto
import boto_config

#User settings
#Location where to store code
code_dir = r'~/code'
#Name of git repo--will be added to code_dir by git
git_repo_name = 'BehanceTopUserFollowers'
#From where to clone the git repo
git_repo_location = r'git://github.com/aravenel/BehanceTopUserFollowers.git'
#git branch to use
git_branch = 'aws'
rabbitmq_user = 'ravenel'
rabbitmq_password = 'sailor'
rabbitmq_vhost = 'myvhost'

aws_access_key_id = boto_config.AWS_ACCESS_KEY_ID
aws_secret_access_key = boto_config.AWS_SECRET_ACCESS_KEY
key_pair_location = boto_config.AWS_KEY_PAIR_LOCATION
key_pair_name = boto_config.AWS_KEY_PAIR_NAME


def dev():
    env.user = 'ubuntu'
    # env.hosts = _get_hosts_by_environment('dev')
    #Only include the celery workers--needed for push/pull
    env.hosts = _get_hosts_by_env_and_type(['celery_worker', 'celery_writer'], 'dev')
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


def _get_instances_by_environment(environment='dev'):
    """Return list of all instance objects with specified
    environment tag value."""
    ec2 = boto.connect_ec2(aws_access_key_id, aws_secret_access_key)
    reservations = ec2.get_all_instances()
    return [i for r in reservations for i in r.instances]


def _get_hosts_by_environment(environment='dev'):
    """Return list of public dns entries of all instances with specified
    environment tag value."""
    instances = _get_instances_by_environment(environment)
    filtered_instances = [i.public_dns_name for i in instances if 
            i.tags.get('environment') == environment and i.state == 'running']
    return filtered_instances


def _get_hosts_by_env_and_type(type_tag, environment='dev'):
    """Return list of public dns entries of all instances with specified
    environment and type tag. type_tag can be a string or list. If it is
    a list, will check to see if the type tag matches any value in list."""
    if type(type_tag) is not list:
        type_tags = [type_tag]
    else:
        type_tags = type_tag

    instances = _get_instances_by_environment(environment)
    filtered_instances = [i.public_dns_name for i in instances if 
            i.tags.get('environment') == environment and i.tags.get('type') in type_tags
            and i.state == 'running']
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
    run('mkdir -p %s' % os.path.join(code_dir, git_repo_name))
    with cd(os.path.join(code_dir, git_repo_name)):
        run('virtualenv venv')
        run('source ./venv/bin/activate')
        run('sudo pip install requests celery')


def config_celery_workers():
    """Deploy and configure code for base celery workers."""
    #Do base-worker specific configuration
    try:
        run('sudo /etc/init.d/celeryd stop')
    except:
        pass

    run('sudo mv init/celeryd /etc/init.d/celeryd')
    run('sudo mv init/celeryd.conf /etc/default/celeryd')

    run('sudo /etc/init.d/celeryd start')


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
    #Provision the EC2 servers using Boto
    print "Provisioning celery workers..."
    provision_celery_workers(n_workers, environment=environment)
    print "Provisioning celery writers..."
    provision_celery_writer(n_writers, environment=environment)
    print "Provisioning rabbitmq server..."
    provision_rabbitmq_server(environment=environment)
    #Setup all the servers
    with settings(parallel=True): #Do these in parallel
        #Rabbitmq server
        print "Configuring rabbitmq server..."
        with settings(host_string = _get_hosts_by_env_and_type('rabbitmq', environment)):
            deploy_rabbitmq()
            config_rabbitmq()
        print "Configuring celery workers..."
        with settings(host_string = _get_hosts_by_env_and_type('celery_worker', environment)):
            deploy_celery()
            pull()
            config_celery_workers()
        #Celery writers
        print "Configuring celery writer..."
        with settings(host_string = _get_hosts_by_env_and_type('celery_writer', environment)):
            deploy_celery()
            pull()
            config_celery_writers()


def deprovision_full_system(environment='dev'):
    print "Terminating celery workers..."
    deprovision_ec2_instances(type_tag='celery_worker', environment=environment)
    print "Terminating celery writers..."
    deprovision_ec2_instances(type_tag='celery_writer', environment=environment)
    print "Terminating rabbitmq server..."
    deprovision_ec2_instances(type_tag='rabbitmq', environment=environment)


def push():
    """Push code from local repo to remote repo"""
    local('git add . && git commit && git push origin %s' % git_branch)


def pull():
    """Checkout code from git onto celery nodes."""
    run('cd %s' % code_dir)
    run('rm -rf *')
    run('git clone %s' % git_repo_location)
    run('git checkout %s' % git_branch)


def pushpull():
    pass
