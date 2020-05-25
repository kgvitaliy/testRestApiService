import json
import pymysql
from flask import Flask, request
from flask import abort

app = Flask(__name__)
# innit db connection
db = pymysql.connect(host='localhost', port=32770, user='root', passwd='12345', db='game')
cur = db.cursor()

#define pots method
@app.route('/game/api/v1.0/member', methods=['POST'])
def get_member_win_stats():
    content = request.get_json()

    if "MEMBER_ID" in content:
        member_id = content['MEMBER_ID']
        where_stm = 'WHERE MEMBER_ID = '+ member_id
    else:
        abort(404)

    if "ACTIVITY_YEAR_MONTH" in content:
        month_numb = content['ACTIVITY_YEAR_MONTH']
        where_stm = where_stm + ' AND ACTIVITY_YEAR_MONTH = ' + month_numb

    if "GAME_ID" in content:
        game_id = content["GAME_ID"]
        where_stm = where_stm + ' AND GAME_ID = ' + game_id

    #sql template
    get_member_stats_by_id = 'SELECT CAST(MEMBER_ID AS CHAR) AS member ,CAST(SUM(WIN_AMOUNT) AS CHAR) AS total_win_amount  ' \
                             ',CAST(SUM(WAGER_AMOUNT) AS CHAR) AS total_wager_amount ' \
                             ',CAST(COUNT(MEMBER_ID) AS CHAR) AS wagers_placed' \
                             ' FROM game_sys.REVENUE_ANALYSIS ' + where_stm + ';'
    rv = cur.execute(get_member_stats_by_id)
    #get the column names from the sql
    row_headers = [x[0] for x in cur.description]  # this will extract row headers
    rv = cur.fetchall()
    #put the resultset into json
    json_data = []
    for result in rv:
       json_data.append(dict(zip(row_headers, result)))
    return json.dumps(json_data)

if __name__ == '__main__':
    app.run(debug=True)