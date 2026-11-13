"""现有接口和关键业务语义检查。"""
import copy
import json
import tempfile
from pathlib import Path
import unittest
from app.storage import freeze,read_json,digest
from app.service import inspect
from app.semantics import evaluate,where

SAMPLE=json.loads((Path(__file__).resolve().parents[1]/'fixtures'/'sample.json').read_text(encoding='utf-8'))
EXPECTED=[{'rid': 'a', 'prefix_sum': '3', 'count_value': 1}, {'rid': 'b', 'prefix_sum': '5', 'count_value': 2}, {'rid': 'c', 'prefix_sum': '5', 'count_value': 2}]

class ExistingBehaviorTests(unittest.TestCase):
    def test_existing_query_result(self):
        actual=inspect(SAMPLE)
        self.assertEqual(actual['result'],EXPECTED)
        self.assertEqual(actual['input_digest'],digest(SAMPLE))

    def test_input_and_output_isolation(self):
        before=copy.deepcopy(SAMPLE)
        frozen=freeze(SAMPLE)
        frozen['unrelated']='changed'
        result=inspect(SAMPLE)
        result['result'].clear()
        self.assertEqual(SAMPLE,before)
        self.assertEqual(inspect(SAMPLE)['result'],EXPECTED)

    def test_input_identity_and_invalid_number(self):
        data=copy.deepcopy(SAMPLE);data['marker']='new'
        self.assertNotEqual(digest(data),digest(SAMPLE))
        data['marker']=float('nan')
        with self.assertRaisesRegex(ValueError,'非有限'):freeze(data)

    def test_json_duplicate_key_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            path=Path(td)/'input.json'
            path.write_text('{"a":1,"a":2}',encoding='utf-8')
            with self.assertRaisesRegex(ValueError,'重复'):read_json(path)

    def test_threevalued_filter(self):
        unknown={'op':'eq','field':'a','value':1}
        self.assertIsNone(evaluate({'a':None},unknown))
        self.assertFalse(where({'a':None},unknown))
        self.assertTrue(evaluate({'a':None},{'op':'is_null','field':'a'}))
        self.assertIsNone(evaluate({'a':None},{'op':'not','arg':unknown}))
        self.assertIs(evaluate({'a':None,'x':0},{'op':'and','args':[unknown,{'op':'eq','field':'x','value':1}]}),False)

    def test_peer_stability_null_order_and_repeated_rid(self):
        data=copy.deepcopy(SAMPLE);data['rows'].reverse()
        self.assertEqual(inspect(data)['result'],EXPECTED)
        data['rows'].append({'rid':'d','partition':'CN','order':None,'value':'1'})
        data['query']['nulls_first']=True
        self.assertEqual(inspect(data)['result'][0],{'rid':'d','prefix_sum':'1','count_value':1})
        data['rows'].append(copy.deepcopy(data['rows'][0]))
        with self.assertRaisesRegex(ValueError,'重复'):inspect(data)

if __name__=='__main__':unittest.main()
