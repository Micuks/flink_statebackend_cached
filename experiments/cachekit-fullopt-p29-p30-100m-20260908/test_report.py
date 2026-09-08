import unittest

from report import COLUMNS, render


def fixture(completed=15):
    queries = ['q4', 'q5', 'q8', 'q9', 'q11', 'q18', 'q19', 'q20',
               'q3', 'q7', 'q12', 'q13', 'q15', 'q16', 'q17']
    results = []
    for i, q in enumerate(queries):
        present = i < completed
        leg = {'measurement': {'raw_throughput_kps': 1200, 'cores': 10},
               'batch_keys': 0, 'copy_lower_bound': 1048576, 'effective': True,
               'jvm_memory_gc': {'queries': {}}} if present else None
        results.append(dict(query=q, rocksdb=50, fullopt=100,
                            optimized=120 if present else None,
                            fullopt_vs_rocksdb=100,
                            optimized_vs_rocksdb=140 if present else None,
                            optimized_vs_fullopt=20 if present else None,
                            leg=leg))
    return {'identity': dict(platform='test', source_commit='test-commit',
                            campaign='new', reference_campaign='fullopt',
                            reused_fullopt_config_sha256='config-hash'),
            'references': {'fullopt': [dict(query=q, raw_kps=1000, cores=10) for q in queries],
                           'rocksdb': {'source_campaign': 'rdb', 'audited_raw_rows':
                                       [dict(query=q, raw_kps=500, cores=10) for q in queries]}},
            'results': results, 'complete': completed == 15, 'valid_legs': completed,
            'effective_queries': queries[:completed],
            'effective_mean_vs_fullopt': 20, 'all_query_mean_vs_fullopt': 20}


class ReportTest(unittest.TestCase):
    def test_native_tables_and_requested_order(self):
        payload = render(fixture())
        tables = [e for e in payload['card']['elements'] if e['tag'] == 'table']
        self.assertEqual(len(tables), 3)
        self.assertEqual([c['name'] for c in tables[0]['columns']], [k for k, _ in COLUMNS])
        self.assertEqual(len(tables[0]['rows']), 18)
        self.assertEqual(tables[0]['rows'][-1]['optimized_vs_fullopt'], '+20.00%')
        self.assertTrue(all(t['page_size'] <= 10 for t in tables))

    def test_partial_missing_and_negative_preserved(self):
        data = fixture(1)
        data['results'][0].update(optimized=90, optimized_vs_fullopt=-10,
                                  optimized_vs_rocksdb=80)
        data['monitoring_status'] = 'Waiting for idle NUMA0'
        payload = render(data)
        table = next(e for e in payload['card']['elements'] if e['tag'] == 'table')
        self.assertEqual(table['rows'][1]['optimized'], '—')
        self.assertEqual(table['rows'][-1]['query'], '15Q partial 1/15')
        self.assertEqual(table['rows'][-1]['optimized_vs_fullopt'], '-10.00%')
        self.assertIn('Waiting for idle NUMA0', payload['card']['elements'][0]['content'])


if __name__ == '__main__':
    unittest.main()
