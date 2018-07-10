import boto3, datetime, sys, signal
from multiprocessing import Pool, Queue, Manager

start_time = datetime.datetime.now()

session = boto3.Session(profile_name='autoclassics')
session = boto3.Session(aws_access_key_id = '<>',aws_secret_access_key = '<>')
s3 = session.resource('s3')
bucket = s3.Bucket('autoclassics')


def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def check_prefix(i,prefix,q):
#    i, prefix = data
    print '%s : Started %s(%d)' % (str(datetime.datetime.now()), prefix, i)
    for obj in bucket.objects.filter(Prefix=prefix):
#	print obj.key
#    	if not idx % 20 and idx > 0 :
#	    print ("Passed %d element in %s" % (idx,prefix) )
#	    break
	if 'CacheControl' not in obj.get():
	    print obj.key
	    q.put(obj.key)
    return 0

m = Manager()
q = m.Queue()
pool = Pool(64,init_worker)

#signal.signal(signal.SIGINT, signal.signal(signal.SIGINT, signal.SIG_IGN))

try:
    results = [pool.apply_async(check_prefix, (i,'prod/car-photos/%s/' % format(i,'03x'),q,)) for i in range(4097)]
    for r in results:
	r.get()
except KeyboardInterrupt:
    print "KB interrupt cathed"
    pool.terminate()

#print pool.map(check_prefix,enumerate(['prod/car-photos/%s/' % format(i,'03x') for i in range(2)]))
print "End\nElapsed time: %s" % str(datetime.datetime.now() - start_time)


with open('./result','w') as fh:
    while not q.empty():
	item = q.get()
	fh.write(item + '\n')

#       s3.meta.client.copy_object(CopySource=obj.bucket_name + '/' + obj.key,
#                   Bucket=obj.bucket_name,
#                   Key=obj.key,
#		   ContentType='image/' + filetype,
#		   CacheControl='max-age=604800',
#                   MetadataDirective='REPLACE')
#       print obj
