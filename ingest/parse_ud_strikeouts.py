import json
import sys
from pathlib import Path
import pandas as pd

def load_json(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))

def parse_strikeout_lines(data: dict) -> list[dict]:
    """
    From the Underdog JSON payload, return a list of dicts:
    {
      player,
      k_line,
      over_american, over_decimal, over_multiplier,
      under_american, under_decimal, under_multiplier
    }
    but only for pitchers marked as starters.
    """

    starter_ids = {
        app['id']
        for app in data.get('appearances', [])
        if any(badge.get('label')=='Starting' and badge.get('value')=='Yes'
               for badge in app.get('badges', []))
    }

    results = []
    for line in data.get('over_under_lines', []):
        appearance_id = (
            line.get('over_under', {})
            .get('appearance_stat', {})
            .get('appearance_id')
        )
        if appearance_id not in starter_ids:
            continue

        player = line['options'][0]['selection_header']
        k_line = line.get('stat_value')

        over_opt = next(opt for opt in line['options'] if opt['choice']=='higher')
        under_opt = next(opt for opt in line['options'] if opt['choice']=='lower')

        results.append({
            'player': player,
            'k_line': k_line,
            'over_american_price': over_opt.get('american_price'),
            'over_decimal_price': over_opt.get('decimal_price'),
            'over_payout_multiplier': over_opt.get('payout_multiplier'),
            'under_american_price': under_opt.get('american_price'),
            'under_decimal_price': under_opt.get('decimal_price'),
            'under_payout_multiplier': under_opt.get('payout_multiplier'),
        })

    df = pd.DataFrame(results)
    df.to_csv("../data/lines/strikeouts.csv", index=False, encoding='utf-8')

    return results

def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <path-to-json-file>")
        sys.exit(1)

    data = load_json(sys.argv[1])
    lines = parse_strikeout_lines(data)

    print(f"{'Player':20} {'K':>4}     {'Over (A/D xM)':20}    {'Under (A/D xM)'}")
    print("-"*70)
    for l in lines:
        over_fmt = f"{l['over_american_price']}/{l['over_decimal_price']}x{l['over_payout_multiplier']}"
        under_fmt = f"{l['under_american_price']}/{l['under_decimal_price']}x{l['under_payout_multiplier']}"
        print(f"{l['player']:20} {l['k_line']:>4}     {over_fmt:20} {under_fmt}")

if __name__ == "__main__":
    main()