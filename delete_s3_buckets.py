import boto3
import sys
import getopt

# 

def get_bucket_names(client):
    bkts = client.list_buckets()
    bkt_names = []
    for b in bkts.get('Buckets'):
        b_name = b.get('Name')
        bkt_names.append(b_name)
    return bkt_names

def filter_bucket_names(bucket_names, find_filter, except_filters=""):
    filtered_bkt_names = []
    for bn in bucket_names:
        if bn.find(find_filter) != -1 and bn not in except_filters:
            filtered_bkt_names.append(bn.strip())
    return filtered_bkt_names
              
def get_bucket_object_keys(client, bucket):
    res = client.list_objects(
            Bucket= bucket,
        )

    bkt_objs = res.get('Contents')
    keys = []
    if bkt_objs != None:
        for o in bkt_objs:
            keys.append(o.get('Key'))
    return keys
        
def delete_bucket_objects(client, bucket_name, object_keys):
    if len(object_keys) == 0:
        return
    keys = []
    for k in object_keys:
        obj = {
            'Key': k
        }
        keys.append(obj)

    res = client.delete_objects(
        Bucket= bucket_name,
        Delete={
            'Objects': keys,
            'Quiet': True|False
        }
    )

def delete_bucket(client, bucket_name):
    response = client.delete_bucket(
        Bucket= bucket_name
    )

def main(argv):
    client = boto3.client('s3')

    FIND_FILTER = ''
    EXCEPT_FILTERS = []
    DECLINE_CONFIRM_DELETE = False

    try:
        opts, args = getopt.getopt(argv, 'hyf:e:',['help','find_filter=','except_filter='])
    except getopt.GetoptError:
        print('test.py  <find_filter> <except_filter>')
        sys.exit(2)

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print('''USAGE
            test.py <find_filter> -o <except_filter>
            find_filter - This is a substring (or complete name) to match to buckets in the account. If we have buckets example-bucket, my-bucket, exam-bucket a find_filter of "exam" will return example-bucket, exam-bucket. For find_filter "m" or "bucket" all buckets would return. For find_filter "m-" only my-bucket would return.
            
            except_filter - This is a comma seperated list of exception exact matches to exclude from the filtered list.
            
            Before deleteing bucket contents and bucket a confirmation for deletion will be asked for unless the "-y" flag is passed.''')
            sys.exit()
        elif opt == '-y':
            DECLINE_CONFIRM_DELETE = True
        elif opt in ('-f', '--find_filter'):
            FIND_FILTER = arg
        elif opt in ('-e', '--except_filter'):
            filters = [] 
            for a in arg.split(','):
                filters.append(a.strip())
            EXCEPT_FILTERS = filters

    BUCKETS = get_bucket_names(client)
    
    FILTERED_BUCKETS = filter_bucket_names(BUCKETS, FIND_FILTER, EXCEPT_FILTERS)

    print('buckets', FILTERED_BUCKETS)
    for b in FILTERED_BUCKETS:
        KEYS = get_bucket_object_keys(client, b)
        SKIP = ''
        if not DECLINE_CONFIRM_DELETE:
            print('bucket', b)
            print('objects', KEYS)
            print('Delete bucket and contents? ( y ) ( n )')
            SKIP = input()
            NORMALIZED_SKIP = SKIP.lower().strip()
            if NORMALIZED_SKIP == 'n':
                print(f'Skipping bucket ( {b} )')
                continue
            elif NORMALIZED_SKIP != 'y':
                print(f'Unrecognized entry ( {SKIP} ) . Skipping bucket ( {b} )')
                continue
        delete_bucket_objects(client, b, KEYS)
        delete_bucket(client, b)
        print(f'Deleted bucket ( {b} ) and contents.\n')
        

if __name__ == "__main__":
   main(sys.argv[1:])

