/** Dependencies ***********************************************************************/
const http = require('http');
const libUrl = require('url');
const Promise = require('bluebird');

const httpGet = url => new Promise((resolve, reject) => http.get(url, res => {
  res.setEncoding('utf8');
  res.on('data', chunk => resolve(JSON.parse(chunk)));
}).on('error', reject));

const logger = {
  info: (...msg) => console.log(...msg),
  error: (...msg) => console.error(...msg)
};

/** Parameters and Utilities ***********************************************************/
/** Configurations * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
const subscription_key = 'f7cc29509a8443c5b3a5e56b0e38b5a6'
const getBopUrl = query => libUrl.format({
  protocol: 'https',
  host: 'oxfordhk.azure-api.net',
  pathname: 'academic/v1.0/evaluate',
  query
});
const single_time_limit = 10000;
const aggressive = false;
const enter_aggressive = () => aggressive = true;
const leave_aggressive = () => aggressive = false;

/** Default settings * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
const default_count = 50;
const default_attrs = ['Id', 'F.FId', 'C.CId', 'J.JId', 'AA.AuId', 'AA.AfId', 'RId'];

/** Utilities for Arrays * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
const mapGet = (prop, array) => array.map(el => el[prop]);
const flatten = s => s.reduce((a, b) => a.concat(b));
const split_list = (list, sub) => {
  const length = list.length;
  const count = length / sub;
  const lists = Array(Math.ceil(count));
  for (let i = 0, j = 0, k = sub; i < count; i++, j = k, k += sub) {
    lists[i] = list.slice(j, k);
  }
  return lists;
};
const mapDouble = (cats, dogs, fn) => {
  const catCount = cats.length;
  const dogCount = dogs.length;
  const pairs = Array(catCount * dogCount);
  for (let i = 0, j = 0; j < catCount; j++) {
    for (let k = 0; k < dogCount; k++, i++) {
      pairs[i] = fn(cats[j], dogs[k]);
    }
  }
  return pairs;
};

/** Utilities for Set  * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
const make_unique = a => [...new Set(a)];
const get_intersection = (a, b) => [...new Set(a.filter(x => b.includes(x)))];
const get_union = (a, b) => new Set([...a, ...b]);
const get_union_all = lists => [...lists.reduce(get_union, [])];

// Utilities and Constants for logic-related stuff
const parse_paper_json = entity => {
  const paper = {
    id: entity.Id,
    fid: 'F' in entity ? mapGet('FId', entity.F) : [],
    cid: 'C' in entity ? [entity.C.CId] : [],
    jid: 'J' in entity ? [entity.J.JId] : [],
    rid: 'RId' in entity ? entity.RId : []
  };
  if ('AA' in entity) {
    paper.auid = mapGet('AuId', entity.AA);
    paper.afid = entity.AA.map(d => 'AfId' in d ? d.AfId : null);
  } else {
    paper.auid = [];
    paper.afid = [];
  }
  return paper;
};
const TYPE_PAPER = 1;
const TYPE_AUTHOR = 2;
const parseTy = entity =>
  'Ti' in entity ? [TYPE_PAPER, parse_paper_json(entity)]
                 : [TYPE_AUTHOR, entity.Id];

/** HTTP IO ****************************************************************************/
const send_http_request = (expr, {count, attributes}={}) => {
  const params = {expr, 'subscription-key': subscription_key};
  if (count) {
    params.count = count;
  }
  if (attributes) {
    params.attributes = attributes.join(',');
  }

  const requestUrl = getBopUrl(params);
  const shoot = () => httpGet(requestUrl).get('entities');

  return aggressive ? Promise.race([
    shoot(),
    shoot(),
    shoot()
  ]) : shoot().timeout(single_time_limit);
};

const get_id_type = (id1, id2) =>
  send_http_request(`Or(Id=${id1},Id=${id2})`, {count: 2, attributes: default_attrs.concat('Ti')})
    .then(resp => {
      if (resp.length === 2) {
        return (resp[0].Id == id1 ? resp : resp.reverse()).map(parseTy);
      }
      if (resp.length === 1) {
        const [entity] = resp;
        const ty = [TYPE_PAPER, parse_paper_json(entity)];
        return entity.Id == id1 ? [ty, [TYPE_AUTHOR, id2]] :
                                  [[TYPE_AUTHOR, id1], ty];
      }
      return [[TYPE_AUTHOR, id1], [TYPE_AUTHOR, id2]];
    });

const fetch_papers = paper_ids => {
  if (paper_ids.length === 0) {
    return Promise.resolve([]);
  }

  const fetch_papers_safe = paper_ids =>
    send_http_request(
      paper_ids.map(a => `Id=${a}`).reduce((a, b) => `Or(${a},${b})`),
      {count: paper_ids.length, attributes: default_attrs}
    )
      .then(resp => {
        if (resp.length !== paper_ids.length) {
          throw `fetched incomplete paper list of ${paper_ids}`;
        }
        const indices = {};
        paper_ids.forEach(paper_id => indices[paper_id] = i);
        const papers = Array(paper_ids.length).fill(null);
        resp.forEach(entity => {
          const paper = parse_paper_json(entity);
          papers[indices[paper.id]] = paper;
        });
        return papers;
      });

  return Promise.all(
    split_list(paper_ids, 23).map(fetch_papers_safe)
  )
    .then(flatten)
    .catch(() => null);
};

const search_papers_by_ref = (rid, {count=default_count, attrs=default_attrs}={}) =>
  send_http_request(`RId=${rid}`, {count, attributes: attrs})
    .then(resp => resp.map(parse_paper_json));
const search_papers_by_author = (auid, {count=default_count, attrs=default_attrs}={}) =>
  send_http_request(`Composite(AA.AuId=${auid})`, {count, attributes: attrs})
    .then(resp => resp.map(parse_paper_json));
const search_papers_and_affiliations_by_author = (auid, {count=default_count, attrs=default_attrs}={}) => {
  const filter_by_auid = (auid_list, afid_list) =>
    afid_list.filter((afid, i) => afid && auid_list[i] == auid);
  return search_papers_by_author(auid, {count, attrs})
    .then(papers => [
      papers,
      get_union_all(papers.map(p => filter_by_auid(p.auid, p.afid)))
    ]);
};
const search_affiliations_by_author = (auid, {count=default_count, au_papers=null}={}) => {
  const filter_by_auid = (auid_list, afid_list) =>
    afid_list.filter((afid, i) => afid && auid_list[i] == auid);
  return Promise.resolve(
    au_papers || search_papers_by_author(auid, {count, attrs: ['Id', 'AA.AuId', 'AA.AfId']})
  )
    .then(au_papers => get_union_all(au_papers.map(p => filter_by_auid(p.auid, p.afid))));
};
const search_paper_ids_by_coauthor = (auid1, auid2, {count=default_count}={}) =>
  send_http_request(`And(Composite(AA.AuId=${auid1}),Composite(AA.AuId=${auid2}))`, {count, attributes: ['Id']})
    .then(resp => resp.map(p => p.Id));
const search_paper_ids_by_author_and_ref = (auid, paper_id, {count=default_count}={}) =>
  send_http_request(`And(Composite(AA.AuId=${auid}),RId=${paper_id})`, {count, attributes: ['Id']})
    .then(resp => resp.map(p => p.Id));
const test_author_in_affiliation = (auid, afid) =>
  send_http_request(`Composite(And(AA.AuId=${auid},AA.AfId=${afid}))`, {count: 1, attributes: ['Id']})
    .then(resp => resp.length === 1);

/** Classes ****************************************************************************/
const pp_solver = {
  prefetch: search_papers_by_ref,
  solve_1hop: (paper1, paper2) => paper1.rid.includes(paper2.id) ? [[paper1.id, paper2.id]] : [],
  solve_2hop: (paper1, paper2, {paper2_refids=null}={}) => {
    const find_way = (list1, list2) =>
      get_intersection(list1, list2).map(x => [paper1.id, x, paper2.id]);

    return Promise.resolve(
      paper2_refids === null ? search_papers_by_ref(paper2.id, {attrs: ['Id']}).call('map', p => p.id) : paper2_refids
    )
      .then(paper2_refids => flatten([
        find_way(paper1.fid, paper2.fid),
        find_way(paper1.cid, paper2.cid),
        find_way(paper1.jid, paper2.jid),
        find_way(paper1.auid, paper2.auid),
        find_way(paper1.rid, paper2_refids)
      ]));
  },
  solve: (paper1, paper2, prefetched=null) =>
    Promise.all(
      Promise.resolve(prefetched === null ? pp_solver.prefetch(paper2.id) : prefetched),
      fetch_papers(paper1.rid)
    )
      .then(([paper2_refs, paper1_refs]) => {
        const paper2_refids = mapGet('id', paper2_refs);
        const search_forward_reference = ref_paper =>
          pp_solver.solve_2hop(ref_paper, paper2, {paper2_refids})
            .then(result => result.map(l => [paper1.id].concat(l)));
        const search_backward_reference = ref_paper =>
          pp_solver.solve_2hop(paper1, ref_paper, {paper2_refids: []})
            .then(result => result.map(l => l.concat([paper2.id])));
        return [
          pp_solver.solve_1hop(paper1, paper2),
          pp_solver.solve_2hop(paper1, paper2, {paper2_refids})
        ]
          .concat(paper1_refs.map(search_forward_reference))
          .concat(paper2_refs.map(search_backward_reference));
      })
};
const aa_solver = {
  prefetch: (auid1, auid2) => Promise.all([
    search_papers_by_author(auid1, {attrs: ['Id','RId','AA.AuId','AA.AfId']}),
    search_papers_by_author(auid2, {attrs: ['Id','AA.AuId','AA.AfId']}),
    search_paper_ids_by_coauthor(auid1, auid2)
  ]),
  solve_1hop: () => [],
  solve_2hop: (auid1, auid2, {au1_papers=null, au2_papers=null, coauthor_paper_ids=null}={}) => {
    const search_by_paper = ({coauthor_paper_ids=null}={}) =>
      Promise.resolve(
        coauthor_paper_ids === null ?
          search_paper_ids_by_coauthor(auid1, auid2) :
          coauthor_paper_ids
      )
        .then(coauthor_paper_ids => coauthor_paper_ids.map(id => [auid1, id, auid2]));
    const search_by_affiliation = () =>
      Promise.all([
        search_affiliations_by_author(auid1, {au_papers: au1_papers}),
        search_affiliations_by_author(auid2, {au_papers: au2_papers})
      ])
        .then(([aff1, aff2]) => get_intersection(aff1, aff2).map(x => [auid1, x, auid2]));
    return Promise.all([
      search_by_paper({coauthor_paper_ids}),
      search_by_affiliation()
    ])
      .then(([way1, way2]) => way1.concat(way2));
  },
  solve: (auid1, auid2, {prefetched=null}={}) =>
    Promise.resolve(
      prefetched !== null ? prefetched : aa_solver.prefetch(auid1, auid2)
    )
      .then(([au1_papers, au2_papers, coauthor_paper_ids]) => {
        const search_bidirection_papers = () => {
          paper_id2 = new Set(au2_papers.map(p => p.id));
          const find = paper =>
            get_intersection(paper.rid, paper_id2)
              .map(id => [auid1, paper.id, id, auid2]);
          return flatten(au1_papers.map(find));
        };
        return [
          aa_solver.solve_1hop(auid1, auid2),
          aa_solver.solve_2hop(auid1, auid2, {au1_papers, au2_papers, coauthor_paper_ids}),
          search_bidirection_papers()
        ];
      })
};
const ap_solver = {
  prefetch: (auid, paper_id) => Promise.all([
    search_papers_and_affiliations_by_author(auid),
    search_papers_by_ref(paper_id, {attrs: ['Id']}),
    search_paper_ids_by_author_and_ref(auid, paper_id)
  ]),
  solve_1hop: (auid, paper) =>
    paper.auid.includes(auid) ? [[auid, paper.id]] : [],
  solve_2hop: (auid, paper, {au_ref_paper_ids=null}={}) =>
    (au_ref_paper_ids || search_paper_ids_by_author_and_ref(auid, paper.id))
      .map(id => [auid, id, paper.id]),
  solve: (auid, paper, {prefetched=null}={}) =>
    Promise.resolve(
      prefetched !== null ? prefetched :
      ap_solver.prefetch(auid, paper.id)
    )
      .then(([[au_papers, affiliations], paper_refs, au_ref_paper_ids]) => {
        const paper_refids = paper_refs.map(p => p.id);
        const search_forward_paper = paper1 =>
          pp_solver.solve_2hop(paper1, paper, {paper2_refids: paper_refids})
            .then(ways => ways.map(l => [auid].concat(l)));
        const search_both_affiliation_and_author = (afid, auid2) =>
          test_author_in_affiliation(auid2, afid)
            .then(bool => bool ? [[auid, afid, auid2, paper.id]] : []);
        return [
          ap_solver.solve_1hop(auid, paper),
          ap_solver.solve_2hop(auid, paper, {au_ref_paper_ids})
        ]
          .concat(au_papers.map(search_forward_paper))
          .concat(mapDouble(
            affiliations, paper.auid,
            (afid, auid2) => search_both_affiliation_and_author(afid, auid2)
          ));
      })
};
const pa_solver = {
  prefetch: search_papers_and_affiliations_by_author,
  solve_1hop: (paper, auid) => paper.auid.includes(auid) ? [[paper.id, auid]] : [],
  solve_2hop: (paper, auid, {au_papers=null}={}) =>
    Promise.resolve(
      au_papers || search_papers_by_author(auid, {attrs: ['Id']})
    )
      .then(au_papers => {
        const rid_set = new Set(paper.rid);
        return au_papers
          .filter(p => rid_set.has(p.id))
          .map(mp => [paper.id, mp.id, auid]);
      }),
  solve: (paper, auid, {prefetched=null}={}) =>
    Promise.resolve(
      prefetched !== null ? prefetched :
      pa_solver.prefetch(auid)
    )
      .then(([au_papers, affiliations]) => {
        const search_backward_paper = paper2 =>
          Promise.all([
            pp_solver.solve_2hop(paper, paper2, {paper2_refids: []}),
            fetch_papers(paper.rid)
          ])
            .then(([ways, paper1_refs]) => {
              if (paper1_refs) {
                Array.prototype.push.apply(
                  ways,
                  paper1_refs
                    .filter(paper1 => paper1.rid.includes(paper2.id))
                    .map(paper1 => [paper.id, paper1.id, paper2.id])
                );
              }
              return ways.map(l => l.concat([auid]));
            });
        const search_both_author_and_affiliation = (auid2, afid) =>
          test_author_in_affiliation(auid2, afid)
            .then(bool => bool ? [[paper.id, auid2, afid, auid]] : []);
        return [
          pa_solver.solve_1hop(paper, auid),
          pa_solver.solve_2hop(paper, auid, {au_papers})
        ]
          .concat(au_papers.map(search_backward_paper))
          .concat(mapDouble(
            affiliations, paper.auid,
            (afid, auid2) => search_both_author_and_affiliation(auid2, afid)
          ));
      })
};

/** Core *******************************************************************************/
const solve = (id1, id2) => {
  // TODO: set_start_time();
  enter_aggressive();

  const tasks = [
    pp_solver.prefetch(id2),
    aa_solver.prefetch(id1, id2),
    ap_solver.prefetch(id1, id2),
    pa_solver.prefetch(id2)
  ];

  const classify = ([[type1, obj1], [type2, obj2]], prefetched, wt) => {
    if (type1 == TYPE_PAPER && type2 == TYPE_PAPER) {
      console.assert(obj1.id == id1 && obj2.id == id2);
      return wt ? pp_solver.solve(obj1, obj2, {prefetched}) : 1;
    } else if (type1 == TYPE_AUTHOR && type2 == TYPE_AUTHOR) {
      console.assert(obj1 == id1 && obj2 == id2);
      return wt ? aa_solver.solve(obj1, obj2, {prefetched}) : 2;
    } else if (type1 == TYPE_AUTHOR && type2 == TYPE_PAPER) {
      console.assert(obj1 == id1 && obj2.id == id2);
      return wt ? ap_solver.solve(obj1, obj2, {prefetched}) : 3;
    } else if (type1 == TYPE_PAPER && type2 == TYPE_AUTHOR) {
      console.assert(obj1.id == id1 && obj2 == id2);
      return wt ? pa_solver.solve(obj1, obj2, {prefetched}) : 4;
    } else {
      return wt ? null : 0;
    }
  };

  return Promise.all([
    get_id_type(id1, id2),
    Promise.any(tasks)
  ]).then(([types, winner]) => {
    const task_index = classify(types, null, false);
    if (task_index === 0) {
      logger.error(`type-unknown found id=${id1},${id2}`);
      leave_aggressive();
      return [];
    }
    return Promise.resolve(tasks[task_index - 1])
      .then(prefetched => {
        logger.info(`solving test (${id1},${show_type(types[0][0])}),(${id2},${show_type(types[1][0])})`);
        return classify(types, prefetched, True);
      })
      .then(fs => {
        leave_aggressive();
        // TODO: set time limit
        return Promise.all(fs);
      })
      .then(done => make_unique(flatten(done)));
  });
};

/** Server *****************************************************************************/
http
  .createServer((req, res) => {
    // TODO: detailed log
    const route = req.url.match(/^\/bop\?id1=(\d+)&id2=(\d+)$/);
    res.end(route ? JSON.stringify(solve(route[1], route[2])) : null);
  })
  .listen(8080, () => {
    logger.info('bop server has started, listening on port 8080');
  });
