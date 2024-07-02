import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
import time

# Used https://www.fangraphs.com/roster-resource/injury-report?timeframe=all&groupby=all&status=&injury=&season=2022 for injury reports
# Now defunct, better method for baseball records was found 

class TooManyRequests(Exception):
    """Too many requests"""

def remove_suffixes(name, suffixes):
    cleaned_name = name
    for suffix in suffixes:
        if cleaned_name.endswith(suffix):
            cleaned_name = cleaned_name[:-len(suffix)].strip()
            break
    return cleaned_name

def get_player_birthday(player_name):
    suffixes = ['Jr.', 'Sr.', 'II', 'III', 'IV', 'V']
    cleaned_name = remove_suffixes(player_name, suffixes)
    names = cleaned_name.split()
    if len(names) < 2:
        print(f"Invalid player name format: {player_name}")
        return None

    last_name = names[-1].lower()[:5]
    first_name = names[0].lower()
    first_two_letters_first_name = first_name[:2]
    first_letter_last_name = last_name[0]
    suffix = "01"

    while True:
        player_url = f"https://www.baseball-reference.com/players/{first_letter_last_name}/{last_name}{first_two_letters_first_name}{suffix}.shtml"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        try:
            response = requests.get(player_url, headers=headers)
            if response.status_code == 429:
                raise TooManyRequests

            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            birth_span = soup.find('span', {'id': 'necro-birth'})
            if birth_span:
                birthday = birth_span['data-birth']
                if int(birthday[:4]) >= 1980:
                    return birthday
                else:
                    print(f"Found birthday before 1980 for {player_name}. Trying next suffix.")
                    suffix = f"{int(suffix) + 1:02}"
            else:
                print(f"No birthday found for {player_name}. Trying next suffix.")
                suffix = f"{int(suffix) + 1:02}"

        except TooManyRequests:
            print(f"Rate limit hit for {player_name}, retrying after delay...")
            time.sleep(30)  # Longer delay for rate limit
            continue
        except requests.exceptions.RequestException as e:
            print(f"Error retrieving data for {player_name}: {e}")
            return None
        except (AttributeError, TypeError) as e:
            print(f"Error parsing data for {player_name}: {e}")
            return None
        time.sleep(10)  # General delay between requests

    return None

def extract_player_names(data):
    df = pd.read_csv(StringIO(data), delimiter='\t')
    player_names = df['Name'].tolist()
    return player_names

data = """Name	Team	Pos	Injury / Surgery Date	Injury / Surgery	Status	IL Retro Date	Eligible to Return	Return Date	Latest Update
Tommy Kahnle	LAD	RP	08/04/20	Tommy John surgery	Activated	04/04/22	04/14/22	05/01/22	Activated
Caleb Ferguson	LAD	RP	09/23/20	Tommy John surgery	Activated	04/04/22	04/14/22	05/16/22	Activated
Nick Mears	PIT	RP	02/09/21	Arthroscopic elbow surgery	Activated	04/04/22	06/03/22	08/20/22	Activated
Kyle Funkhouser	DET	RP	Mar '22	Strained shoulder	60-Day IL	04/04/22	06/03/22	10/06/22	Out for 2022 season
Blake Cederlind	PIT	RP	03/23/21	Tommy John surgery	60-Day IL	04/04/22	06/03/22	10/06/22	No timetable for return
Kirby Yates	ATL	RP	03/24/21	Tommy John surgery	Activated	04/04/22	06/03/22	08/10/22	Activated
Scott Oberg	COL	RP	03/25/21	Blood clot removal surgery (right elbow)	60-Day IL	04/04/22	06/03/22	10/06/22	No timetable for return
Michel Baez	SDP	RP	03/30/21	Tommy John surgery	Activated	04/04/22	06/03/22	06/06/22	Activated
José Leclerc	TEX	RP	03/30/21	Tommy John surgery	Activated	04/04/22	06/03/22	06/16/22	Activated
Jonathan Hernández	TEX	RP	04/12/21	Tommy John surgery	Activated	04/04/22	06/03/22	07/15/22	Activated
Sam Delaplane	SFG	RP	04/13/21	Tommy John surgery	60-Day IL	06/21/22	08/20/22	10/06/22	No timetable for return
James Paxton	BOS	SP	04/14/21	Tommy John surgery	60-Day IL	04/04/22	06/03/22	10/06/22	Out for 2022 season
Adrian Morejon	SDP	RP	04/20/21	Tommy John surgery	Activated	04/04/22	06/03/22	06/06/22	Activated
Carlos Vargas	CLE	SP	04/29/21	Tommy John surgery	Activated	04/04/22	06/03/22	07/18/22	Activated
Dustin May	LAD	SP	05/11/21	Tommy John surgery	Activated	04/04/22	06/03/22	08/20/22	Activated
Michael Soroka	ATL	SP	05/17/21	Achilles' tendon surgery (exploratory)	Activated	04/04/22	06/03/22	09/01/22	Activated
JoJo Romero	PHI	RP	05/25/21	Tommy John surgery	Activated	04/04/22	06/03/22	07/15/22	Activated
Kent Emanuel	PHI	RP	06/03/21	Elbow surgery	Activated	04/04/22	06/03/22	07/08/22	Activated
Kyle Lewis	SEA	OF	06/11/21	Knee surgery (torn meniscus)	Activated	04/04/22	04/14/22	05/24/22	Activated
Justin Dunn	CIN	SP	06/17/21	Strained shoulder	Activated	04/04/22	06/03/22	07/24/22	Activated
Joey Lucchesi	NYM	SP	06/24/21	Tommy John surgery	60-Day IL	04/04/22	06/03/22	10/06/22	Rehab assignment (8/21)
Josh Naylor	CLE	1B/OF	07/02/21	Ankle surgery	Activated	04/04/22	04/14/22	04/15/22	Activated
Ronald Acuña Jr.	ATL	OF	07/21/21	Knee surgery (torn ACL)	Activated	04/04/22	04/14/22	04/28/22	Activated
Stephen Strasburg	WSN	SP	07/28/21	Thoracic outlet syndrome surgery	Activated	04/04/22	04/14/22	06/09/22	Activated
Spencer Turnbull	DET	SP	07/29/21	Tommy John surgery	60-Day IL	04/04/22	06/03/22	10/06/22	Out for 2022 season
Jimmy Nelson	LAD	RP	Aug '21	Tommy John surgery	60-Day IL	04/04/22	06/03/22	10/06/22	Questionable for 2022 season
Drew Pomeranz	SDP	RP	Aug '21	Flexor tendon surgery	60-Day IL	04/04/22	06/03/22	10/06/22	No timetable for return
Tyler Glasnow	TBR	SP	08/04/21	Tommy John surgery	Activated	04/04/22	06/03/22	09/28/22	Activated
Jonathan Stiever	CHW	SP	08/23/21	Lat surgery	60-Day IL	04/04/22	06/03/22	10/06/22	Rehab assignment (9/21)
Tejay Antone	CIN	RP	08/27/21	Tommy John surgery	60-Day IL	04/04/22	06/03/22	10/06/22	Doubtful for 2022 season
Justin Topa	MIL	RP	Sep '21	Flexor tendon surgery	Activated	04/04/22	06/03/22	08/13/22	Activated
Kenta Maeda	MIN	SP	09/01/21	Tommy John surgery	60-Day IL	04/04/22	06/03/22	10/06/22	Out for 2022 season
John Curtiss	NYM	RP	Sep '21	Tommy John surgery	60-Day IL	04/04/22	06/03/22	10/06/22	Questionable for 2022 season
Yonny Chirinos	TBR	SP	Sep '21	Fractured elbow	Activated	04/04/22	06/03/22	09/06/22	Activated
Randy Dobnak	MIN	SP/RP	09/03/21	Strained finger	Activated	04/04/22	06/03/22	09/14/22	Activated
Jake Rogers	DET	C	09/08/21	Tommy John surgery	60-Day IL	04/04/22	06/03/22	10/06/22	Questionable for 2022 season
Zack Britton	NYY	RP	09/08/21	Tommy John surgery	Activated	04/04/22	06/03/22	09/22/22	Activated
Nick Anderson	TBR	RP	09/26/21	Strained back	Activated	04/04/22	06/03/22	08/22/22	Activated
Matthew Boyd	SEA	SP	09/27/21	Flexor tendon surgery	Activated	04/04/22	06/03/22	09/01/22	Activated
Chris Rodriguez	LAA	SP/RP	Oct '21	Shoulder surgery	60-Day IL	04/04/22	06/03/22	10/06/22	Out for 2022 season
Danny Duffy	LAD	RP	Oct '21	Flexor tendon surgery	60-Day IL	04/04/22	06/03/22	10/06/22	No timetable for return
Lance McCullers Jr.	HOU	SP	10/12/21	Strained flexor tendon	Activated	04/04/22	06/03/22	08/13/22	Activated
Joe Kelly	CHW	RP	10/21/21	Strained biceps	Activated	04/04/22	04/14/22	05/09/22	Activated
Tommy La Stella	SFG	INF	10/26/21	Achilles inflammation	Activated	04/04/22	04/14/22	05/16/22	Activated
David Bote	CHC	INF/OF	Nov '21	Shoulder surgery	Activated	04/04/22	06/03/22	06/24/22	Activated
Jake Meyers	HOU	OF	11/10/21	Shoulder surgery (torn labrum)	Activated	04/04/22	06/03/22	06/24/22	Activated
Brendan McKay	TBR	SP	11/23/21	Thoracic outlet syndrome surgery	Activated	04/04/22	06/03/22	08/23/22	Activated
Adbert Alzolay	CHC	SP	Feb '22	Strained lat	Activated	04/04/22	06/03/22	09/17/22	Activated
Lucas Sims	CIN	RP	Feb '22	Elbow/back soreness	Activated	04/04/22	04/14/22	04/22/22	Activated
Griffin Canning	LAA	SP	Feb '22	Back discomfort	60-Day IL	04/04/22	06/03/22	10/06/22	Out for 2022 season
Alex Reyes	STL	RP	Feb '22	Shoulder soreness (frayed labrum)	60-Day IL	04/04/22	06/03/22	10/06/22	Questionable for 2022 season
Jack Flaherty	STL	SP	Feb '22	Shoulder bursitis	Activated	04/04/22	06/03/22	06/15/22	Activated
Nick Ahmed	ARI	SS	Mar '22	Shoulder discomfort	Activated	04/04/22	04/14/22	04/22/22	Activated
Jay Jackson	ATL	RP	Mar '22	Strained lat	Activated	04/04/22	06/03/22	07/02/22	Activated
Chris Sale	BOS	SP	Mar '22	Stress fracture -- rib cage	Activated	04/04/22	06/03/22	07/12/22	Activated
Josh Taylor	BOS	RP	Mar '22	Lower back discomfort	60-Day IL	04/04/22	06/03/22	10/06/22	Out for 2022 season
Wade Miley	CHC	SP	Mar '22	Elbow inflammation	Activated	04/04/22	04/14/22	05/10/22	Activated
Andrelton Simmons	CHC	SS	Mar '22	Shoulder soreness	Activated	04/04/22	04/14/22	05/15/22	Activated
Mike Minor	CIN	SP	Mar '22	Shoulder soreness	Activated	04/04/22	04/14/22	06/03/22	Activated
Luis Castillo	CIN	SP	Mar '22	Shoulder soreness	Activated	04/04/22	04/14/22	05/09/22	Activated
Cody Morris	CLE	SP	Mar '22	Strained shoulder (teres major)	Activated	04/04/22	06/03/22	09/01/22	Activated
James Karinchak	CLE	RP	Mar '22	Strained shoulder (teres major)	Activated	04/04/22	06/03/22	07/02/22	Activated
Peter Lambert	COL	RP	Mar '22	Forearm inflammation	Activated	04/04/22	04/14/22	05/26/22	Activated
Helcris Olivarez	COL	SP	Mar '22	Strained shoulder	60-Day IL	05/28/22	07/27/22	10/06/22	No timetable for return
Cooper Criswell	LAA	SP	Mar '22	Shoulder soreness	Activated	04/04/22	06/03/22	07/15/22	Activated
Dylan Floro	MIA	RP	Mar '22	Rotator cuff tendinitis	Activated	04/04/22	04/14/22	05/09/22	Activated
José Devers	MIA	INF	Mar '22	Shoulder impingement syndrome	Activated	04/04/22	04/14/22	05/16/22	Activated
Jake Reed	NYM	RP	Mar '22	Oblique soreness	Activated	04/04/22	04/14/22	04/20/22	Activated
Domingo Germán	NYY	SP	Mar '22	Shoulder impingement syndrome	Activated	04/04/22	06/03/22	07/21/22	Activated
Stephen Ridings	NYY	RP	Mar '22	Shoulder impingement	60-Day IL	04/04/22	06/03/22	10/06/22	Rehab assignment (9/17)
James Kaprielian	OAK	SP	Mar' 22	AC joint inflammation	Activated	04/04/22	04/14/22	05/01/22	Activated
Brent Honeywell	OAK	SP	Mar' 22	Olecranon stress fracture (right elbow)	Activated	04/04/22	06/03/22	09/11/22	Activated
Odúbel Herrera	PHI	OF	Mar' 22	Strained oblique	Activated	04/04/22	04/14/22	04/22/22	Activated
Rafael Marchán	PHI	C	Mar' 22	Strained hamstring	Activated	04/04/22	06/03/22	06/12/22	Activated
Luis Oviedo	PIT	SP	Mar' 22	Sprained ankle	Activated	04/04/22	04/14/22	04/18/22	Activated
Max Kranick	PIT	SP	Mar' 22	Strained forearm	Activated	04/04/22	04/14/22	04/29/22	Activated
Casey Sadler	SEA	RP	Mar '22	Shoulder surgery	60-Day IL	04/04/22	06/03/22	10/06/22	Out for 2022 season
Seth Romero	WSN	SP	Mar '22	Strained calf	Activated	04/04/22	06/03/22	08/27/22	Activated
Codi Heuer	CHC	RP	03/07/22	Tommy John surgery	60-Day IL	04/04/22	06/03/22	10/06/22	Out for 2022 season
Fernando Tatis Jr.	SDP	SS	03/16/22	Wrist surgery	Activated	04/04/22	06/03/22	08/12/22	Activated
J.B. Bukauskas	ARI	RP	03/17/22	Strained shoulder (teres major)	Activated	04/07/22	06/06/22	07/20/22	Activated
Tommy Nance	CHC	RP	03/19/22	Undisclosed	Activated	03/19/22	03/20/22	03/25/22	Activated
Luis Urías	MIL	INF	03/19/22	Strained quad	Activated	04/04/22	04/14/22	05/02/22	Activated
Luis García	SDP	RP	03/21/22	Side discomfort	Activated	04/04/22	04/14/22	04/15/22	Activated
Shane Baz	TBR	SP	03/21/22	Arthroscopic elbow surgery	Activated	04/04/22	06/03/22	06/11/22	Activated
Jose Barrero	CIN	SS	03/22/22	Hand surgery (hamate)	Activated	04/04/22	04/14/22	06/06/22	Activated
Luke Maile	CLE	C	03/23/22	Strained hamstring	Activated	04/04/22	04/14/22	04/25/22	Activated
Taylor Jones	HOU	INF/OF	03/23/22	Back discomfort	Activated	04/04/22	06/03/22	06/23/22	Activated
Sam Coonrod	PHI	RP	03/23/22	Strained shoulder	Activated	04/04/22	06/03/22	08/15/22	Activated
Alec Mills	CHC	SP	03/25/22	Strained lower back	Activated	04/04/22	06/03/22	06/07/22	Activated
Ken Giles	SEA	RP	03/25/22	Finger discomfort	Activated	04/04/22	06/03/22	06/20/22	Activated
Andrew Chafin	DET	RP	03/26/22	Strained groin	Activated	04/04/22	04/14/22	04/26/22	Activated
Cody Stashak	MIN	RP	03/26/22	Strained biceps	Activated	04/04/22	04/14/22	04/17/22	Activated
Anthony Alford	PIT	OF	03/26/22	Sprained wrist	Activated	04/04/22	04/14/22	04/22/22	Activated
Ryan Borucki	TOR	RP	03/26/22	Strained hamstring	Activated	04/04/22	04/14/22	04/17/22	Activated
Pete Fairbanks	TBR	RP	03/27/22	Strained lat	Activated	04/04/22	06/03/22	07/17/22	Activated
Jordan Luplow	ARI	OF/1B	03/28/22	Strained oblique	Activated	04/04/22	04/14/22	04/25/22	Activated
Yermín Mercedes	CHW	C/OF	03/28/22	Hand surgery (hamate)	Activated	04/04/22	04/14/22	05/11/22	Activated
Ryan Sherriff	PHI	RP	03/28/22	Biceps tendinitis	Activated	04/04/22	06/03/22	08/01/22	Activated
LaMonte Wade Jr.	SFG	OF	03/28/22	Knee inflammation	Activated	04/04/22	04/14/22	05/06/22	Activated"""

player_names = extract_player_names(data)
birthdays = {}
for player_name in player_names:
    birthday = get_player_birthday(player_name)
    birthdays[player_name] = birthday

df = pd.DataFrame(list(birthdays.items()), columns=['Name', 'Birthday'])
print(df)
