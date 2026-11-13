"""现有执行路径。"""
from decimal import Decimal
from functools import cmp_to_key

def compare(a,b,q):
    x,y=a['order'],b['order']
    if x is None or y is None:
        if x is y: c=0
        else: c=(-1 if x is None else 1)*(1 if q['nulls_first'] else -1)
    else:
        x,y=Decimal(x),Decimal(y); c=(x>y)-(x<y)
        if not q['ascending']: c=-c
    return c or ((a['rid']>b['rid'])-(a['rid']<b['rid']))

def execute(data):
    groups={}; seen=set()
    for r in data['rows']:
        if r['rid'] in seen: raise ValueError('行键重复')
        seen.add(r['rid']); groups.setdefault(r['partition'],[]).append(r)
    result=[]
    for p,rows in sorted(groups.items()):
        total=Decimal(0); count=0
        for r in sorted(rows,key=cmp_to_key(lambda a,b:compare(a,b,data['query']))):
            if r['value'] is not None: total+=Decimal(r['value']); count+=1
            result.append({'rid':r['rid'],'prefix_sum':str(total) if count else None,'count_value':count})
    return result
