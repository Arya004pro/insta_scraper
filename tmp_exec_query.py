import requests,time,json,csv,pathlib,sys
base='http://127.0.0.1:8000'
try:
 h=requests.get(base+'/health',timeout=10)
except Exception as e:
 print(json.dumps({'error':'health_check_failed','detail':str(e)})); sys.exit(0)
if h.status_code!=200:
 print(json.dumps({'error':'health_not_ok','status':h.status_code,'body':h.text[:200]})); sys.exit(0)
rid=requests.post(base+'/v1/runs/start',json={'input_type':'single_url','input_value':'https://www.instagram.com/indriyajewels/','use_saved_session':True},timeout=30).json()['run_id']
status='running'
for _ in range(300):
 d=requests.get(base+f'/v1/runs/{rid}',timeout=30).json(); status=d.get('status')
 if status in {'completed','failed','needs_human','skipped_private'}: break
 time.sleep(5)
rep=requests.get(base+f'/v1/runs/{rid}/report',timeout=30).json(); out=rep.get('outputs',{}); samples=rep.get('samples',{}); arts=rep.get('artifacts',{})
posts_path=pathlib.Path((arts.get('posts_csv') or {}).get('path','')); reels_path=pathlib.Path((arts.get('reels_csv') or {}).get('path',''))
posts=[]; reels=[]
if posts_path.exists():
 with posts_path.open('r',encoding='utf-8',newline='') as f: posts=list(csv.DictReader(f))
if reels_path.exists():
 with reels_path.open('r',encoding='utf-8',newline='') as f: reels=list(csv.DictReader(f))
def pick(bucket):
 for r in posts+reels:
  if (r.get('sample_bucket') or '').strip()==bucket:
   return {k:r.get(k) for k in ['shortcode','media_type','sample_bucket','missing_reason_post','likes_count','comments_count']}
 return None
result={'run_id':rid,'status':status,'total':out.get('total_count'),'posts_count':len(posts),'reels_count':len(reels),'samples':{k:bool(v) for k,v in (samples or {}).items()},'single_post_sample':pick('posts'),'multi_image_sample':pick('multi_image_posts'),'reel_sample':pick('reels')}
print(json.dumps(result,ensure_ascii=False,indent=2))
