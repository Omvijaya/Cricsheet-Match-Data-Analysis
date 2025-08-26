import json
import os
import pandas as pd
def parse_match_file(file_path):
    with open(file_path,'r',encoding='utf-8')as f:
        data=json.load(f)
    info=data.get('info',{})
    match_type=info.get('match_type','unknown')
    if match_type.lower() == "t20":
        event = info.get("event", {}).get("name", "")
        competition = info.get("competition", "")
        if event.lower() == "indian premier league" or competition.lower() == "ipl":
            match_type = "IPL"

    match_id=os.path.splitext(os.path.basename(file_path))[0]
    summary={
        'match_id':match_id,
        'date': info.get('dates',[None])[0],
        'venue':info.get ('venue'),
        'city':info.get('city'),
        'team1':info.get('teams',[None,None])[0],
        'team2':info.get('teams',[None,None])[1] if len(info.get('teams',[]))>1 else None,
        'toss_winner':info.get('toss',{}).get('winner'),
        'toss_decision':info.get('toss',{}).get('decision'),
        'match_winner':info.get('outcome',{}).get('winner'),
        'win_by_runs':info.get('outcome',{}).get('by',{}).get('runs',0),
        'win_by_wickets':info.get('outcome',{}).get('by',{}).get('wickets',0),
        'player_of_match':', '.join(info.get('player_of_match',[])),
        'match_type':match_type,
        'overs':info.get('overs')
    }
    deliveries=[]
    for inning_number,inning in enumerate(data.get('innings',[]),start=1):
        batting_team=inning.get('team')
        for over in inning.get('overs',[]):
            over_num=over.get('over')
            for ball_num, delivery in enumerate(over.get('deliveries', []), start=1):
                d={
                    'match_id':match_id,
                    'inning':inning_number,
                    'over':over_num,
                    'ball':ball_num,
                    'batting_team':batting_team,
                    'batsman':delivery.get('batter'),
                    'bowler':delivery.get('bowler'),
                    'runs_batter':delivery.get('runs',{}).get('batter',0),
                    "runs_extras": delivery.get("runs", {}).get("extras", 0),
                    'runs_total':delivery.get('runs',{}).get('total',0),
                    'extras_type':next(iter(delivery.get('extras',{}).keys()),None) if 'extras' in delivery else None,
                    'is_wicket': 1 if 'wickets' in delivery else 0,
                    'wicket_kind':delivery.get('wickets',[{}])[0].get('kind') if 'wickets' in delivery else None,
                    'player_out':delivery.get('wickets',[{}])[0].get('player_out') if 'wickets' in delivery else None
                }
                deliveries.append(d)
    return summary,deliveries,match_type
def process_all_matches(json_folder):
    summaries={'Test':[],'ODI':[],'T20':[],'IPL':[]}
    deliveries={'Test':[],'ODI':[],'T20':[],'IPL':[]}
    for file in os.listdir(json_folder):
        if not file.endswith('.json'):
            continue
        filepath=os.path.join(json_folder,file)
        try:
            summary,delivery_list,match_type=parse_match_file(filepath)
            if match_type.lower()=='test':
                summaries['Test'].append(summary)
                deliveries['Test'].extend(delivery_list)
            elif match_type.lower() == "odi":
                summaries["ODI"].append(summary)
                deliveries["ODI"].extend(delivery_list)
            elif match_type.lower() == "t20":
                summaries["T20"].append(summary)
                deliveries["T20"].extend(delivery_list)
            elif match_type.lower() == "ipl":
                summaries["IPL"].append(summary)
                deliveries["IPL"].extend(delivery_list)
            else:
                pass 
        except Exception as e:
            print(f'failed_parsing{file}:{e}')
    test_matches_df=pd.DataFrame(summaries['Test'])
    test_deliveries_df=pd.DataFrame(deliveries['Test'])
    odi_matches_df = pd.DataFrame(summaries["ODI"])
    odi_deliveries_df = pd.DataFrame(deliveries["ODI"])
    t20_matches_df = pd.DataFrame(summaries["T20"])
    t20_deliveries_df = pd.DataFrame(deliveries["T20"])
    ipl_matches_df = pd.DataFrame(summaries["IPL"])
    ipl_deliveries_df = pd.DataFrame(deliveries["IPL"])
    return (
        test_matches_df,test_deliveries_df,
        odi_matches_df,odi_deliveries_df,
        t20_matches_df,t20_deliveries_df,
        ipl_matches_df,ipl_deliveries_df
)
if __name__=='__main__':
    json_folder='D:/GUVI AIML/Projects/Cricsheet Match Data Analysis/data/all_matches'
    (test_matches, test_deliveries,
     odi_matches, odi_deliveries,
     t20_matches, t20_deliveries,
     ipl_matches, ipl_deliveries) = process_all_matches(json_folder)

    outdir='output'
    os.makedirs(outdir, exist_ok=True)
    test_matches.to_csv(f"{outdir}/test_matches.csv", index=False)
    test_deliveries.to_csv(f"{outdir}/test_deliveries.csv", index=False)
    odi_matches.to_csv(f"{outdir}/odi_matches.csv", index=False)
    odi_deliveries.to_csv(f"{outdir}/odi_deliveries.csv", index=False)
    t20_matches.to_csv(f"{outdir}/t20_matches.csv", index=False)
    t20_deliveries.to_csv(f"{outdir}/t20_deliveries.csv", index=False)
    ipl_matches.to_csv(f"{outdir}/ipl_matches.csv", index=False)
    ipl_deliveries.to_csv(f"{outdir}/ipl_deliveries.csv", index=False)
    print('All Data Frames created and saved to CSV')