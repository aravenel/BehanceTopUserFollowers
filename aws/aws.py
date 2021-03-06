import boto
import os
import time
import boto_config

aws_access_key_id = boto_config.AWS_ACCESS_KEY_ID
aws_secret_access_key = boto_config.AWS_SECRET_ACCESS_KEY

if __name__ == "__main__":

    #Location to which to save the SSH key generated by Amazon
    key_pair_name = 'boto-key'
    key_pair_location = os.path.split(os.path.abspath(__file__))[0]

    print "Connecting to EC2...",
    ec2 = boto.connect_ec2(aws_access_key_id, aws_secret_access_key) 
    print "Done."
    print "Getting and saving keypair...",
    if os.path.isfile(os.path.join(key_pair_location, key_pair_name + ".pem")):
        print "Key already exists..."
    else:
        key_pair = ec2.create_key_pair(key_pair_name)
        key_pair.save(key_pair_location)
    print "Done."

    print "Reserving instances...",
    reservation = ec2.run_instances(
            image_id = 'ami-82fa58eb',
            key_name = 'boto-key',
            instance_type = 't1.micro',
            placement = 'us-east-1a',
            security_groups = ['default'],
            )
    instance = reservation.instances[0]
    print "Done."

    print "Waiting on instance to start..."
    status = instance.update()
    while status == 'pending':
        time.sleep(10)
        status = instance.update()
    if status == 'running':
        print "Done."
        print "New instance " + instance.id + " accessible at " + instance.public_dns_name
    else:
        print "Done."
        print "Instance status: %s" % instance.status
