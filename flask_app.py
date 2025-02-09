import os
from flask import Flask, request, jsonify

app = Flask(__name__)

from database_query_utils import DBRecord
from flask_end_points_service import (get_json_format, set_json_format, get_token_based_authentication, get_app_configurations,
                                      update_audio_transcribe_table, copy_audio_files_process, update_audio_transcribe_tracker_table,
                                      get_client_master_table_configurations, get_audio_transcribe_tracker_table_data, get_file_name_pattern,open_ai_transcribe_audio,
                                      get_ldap_authentication, get_audio_transcribe_table_data, update_transcribe_audio_text, get_all_configurations_table)


db_instance = DBRecord()
server_name = 'FLM-VM-COGAIDEV'
# server_name = '10.9.91.137'
database_name = 'AudioTrans'


@app.route('/get_all_data', methods=['GET'])
def get_record():
    table_name = request.args.get('table_name')
    client_id = int(request.args.get('clientid'))
    data = db_instance.get_all_record(server_name, database_name, client_id,table_name.capitalize())
    return data


@app.route('/get_record_by_id', methods=['GET'])
def get_recordby_id():
    table_name = request.args.get('table_name')
    client_id = int(request.args.get('clientid'))
    id = request.args.get('id')
    data = db_instance.get_record_by_id(server_name, database_name, client_id,
                table_name, id)
    if not data:
        data = {"Error": "Invalid table/Data not available for this " + table_name}
    return data


@app.route('/get_record_by_column_name', methods=['GET'])
def get_recordby_column_name():
    table_name = request.args.get('table_name')
    client_id = int(request.args.get('clientid'))
    column_name = request.args.get('column_name')
    column_value = request.args.get('column_value')
    data = db_instance.get_data_by_column_name(server_name, database_name,
        client_id,table_name, column_name, column_value)
    return data


@app.route('/update_record_by_column', methods=['GET'])
def get_update_by_column_name():
    table_name = request.args.get('table_name')
    client_id = int(request.args.get('clientid'))
    column_to_update = request.args.get('column_to_update')
    new_value = request.args.get('new_value')
    condition_column = request.args.get('condition_column')
    condition_value = request.args.get('condition_value')

    data = db_instance.update_record_by_column(server_name, database_name, client_id,
            table_name, column_to_update, new_value, condition_column,
            condition_value
    )

    if data == None:
        data = {"Error": "Invalid table/Data not available for this " + table_name}
    return data


@app.route('/delete_record_by_id', methods=['DELETE'])
def delete_recordby_id():
    table_name = request.args.get('table_name')
    client_id = int(request.args.get('clientid'))
    itm_id = request.args.get('id')
    data = db_instance.delete_record_by_id(server_name, database_name, client_id,table_name, itm_id)

    # data = db_instance.delete_record_by_id(table_name, itm_id)
    if not data:
        data = {"Error": "Invalid table/Data not available for this " + table_name}
    return {'data': data}


@app.route('/merge_chunk_transcribe_text', methods=['GET'])
def get_transcribe_sentiment():
    from app.model.sentiment_analysis import SentimentAnalysisCreation
    sentiment_instance = SentimentAnalysisCreation()
    client_id = int(request.args.get('clientid'))
    audio_file_name = request.args.get('audio_file')
    data = sentiment_instance.get_data_from_transcribe_table(server_name, database_name, client_id,audio_file_name)
    if not data:
        data = {"Error": "Invalid table/Data not available for this " + audio_file_name}
    return {'data': data}


@app.route('/get_client_master_configurations', methods=['GET'])
def get_client_master_configurations():
    # Done
    client_id = int(request.args.get('clientid'))
    json_result = get_client_master_table_configurations(server_name, database_name, client_id)
    return json_result


@app.route('/get_client_configurations', methods=['GET'])
def get_client_configurations():
    # Done
    client_id = int(request.args.get('clientid'))
    json_result = get_app_configurations(server_name, database_name, client_id)
    return json_result


@app.route('/get_audio_transcribe_data', methods=['GET'])
def get_audio_transcribe_data():
    try:
        # Done
        client_id = int(request.args.get('clientid'))
        json_result = get_audio_transcribe_table_data(server_name, database_name, client_id)
        return json_result
    except Exception as e:
        return get_json_format([], False, e)


@app.route('/get_audio_transcribe_tracker_data', methods=['GET'])
def get_audio_transcribe_tracker_data():
    try:
        # Done
        client_id = int(request.args.get('clientid'))
        audio_id = int(request.args.get('audioid'))
        current_user = os.getlogin()
        json_result = get_audio_transcribe_tracker_table_data(server_name, database_name, client_id, audio_id)
        return json_result
    except Exception as e:
        return get_json_format([], False, e)


@app.route('/add_update_transcribe', methods=['GET'])
def add_update_transcribe():
    #  Dev Done, testing pending
    client_id = int(request.args.get('clientid'))
    recored_id = int(request.args.get('id'))
    updatevalues = request.args.get('updatevalues')
    update_status = update_audio_transcribe_table(server_name, database_name, client_id, recored_id, updatevalues)
    return update_status


@app.route('/add_update_transcribe_tracker', methods=['GET'])
def add_update_transcribe_tracker():
    #  Dev Done, testing pending
    client_id = int(request.args.get('clientid'))
    recored_id = int(request.args.get('id'))
    updatevalues = request.args.get('updatevalues')
    update_status = update_audio_transcribe_tracker_table(server_name, database_name, client_id, recored_id,
        updatevalues)
    return update_status


@app.route('/get_token_based_authenticate', methods=['GET'])
def get_token_based_authenticate():
    #  Dev Done, testing pending
    client_id = int(request.args.get('clientid'))
    user_name = request.args.get('username')
    current_user = os.getlogin()
    print('Current login user:', current_user)
    success, message = get_token_based_authentication(server_name, database_name, client_id, user_name)
    if success:
        return set_json_format([], True, str(message))
    else:
        return set_json_format([], False, str(message))


@app.route('/get_ldap_based_authenticate', methods=['GET'])
def get_ldap_based_authenticate():
    #  Dev Done, testing pending
    client_id = int(request.args.get('clientid'))
    current_user = os.getlogin()
    print('Current login user:', current_user)
    success, message = get_ldap_authentication(server_name, database_name, client_id)
    if success:
        return set_json_format([], True, str(message))
    else:
        return set_json_format([], False, str(message))


@app.route('/get_data_from_sentiment_table', methods=['GET'])
def get_sentiment_data():
    from app.model.sentiment_analysis import SentimentAnalysisCreation
    sentiment_instance = SentimentAnalysisCreation()
    client_id = int(request.args.get('clientid'))
    audio_file_name = request.args.get('audio_file')
    data = sentiment_instance.get_sentiment_data_from_table(server_name, database_name, client_id,audio_file_name)
    if not data:
        data = {"Error": f"File not exit {audio_file_name}"}
    return {'data': data}


@app.route('/get_all_configurations', methods=['GET'])
def get_all_configurations():
    #  Dev Done
    client_id = int(request.args.get('clientid'))
    current_user = os.getlogin()
    print('Current login user:', current_user)
    json_result = get_all_configurations_table(server_name, database_name, client_id)
    return json_result

@app.route('/copy_audio_files', methods=['GET'])
def copy_audio_files():
    #  Dev Done
    client_id = int(request.args.get('clientid'))
    current_user = os.getlogin()
    print('Current login user:', current_user)
    json_result = copy_audio_files_process(server_name, database_name, client_id)
    return json_result

@app.route('/transcribe_audio_text', methods=['GET'])
def transcribe_audio_text():
    #  Dev Dones
    client_id = int(request.args.get('clientid'))
    record_id = int(request.args.get('id'))
    current_user = os.getlogin()
    print('Current login user:', current_user)
    json_result = update_transcribe_audio_text(server_name, database_name, client_id, record_id)
    return json_result


@app.route('/match_file_name_pettern', methods=['GET'])
def match_file_name_pettern():
    #  Dev Done
    client_id = int(request.args.get('clientid'))
    file_name = request.args.get('filename')
    current_user = os.getlogin()
    file_name = 'ABC-21March-AY-Noida-Call-Approva-Ashutosh'
    file_name = 'ABC-21March-AY-Noida-Call-Approva'
    print('Current login user:', current_user)
    json_result = get_file_name_pattern(server_name, database_name, client_id,file_name)
    return json_result

@app.route('/dump_data_into_sentiment', methods=['GET','POST'])
def dump_data_sentiment_table():
    from app.model.sentiment_analysis import SentimentAnalysisCreation
    sentiment_instance = SentimentAnalysisCreation()
    client_id = int(request.args.get('clientid'))
    audio_file_name = request.args.get('audio_file')
    data = sentiment_instance.get_transcribe_data_for_sentiment(server_name, database_name, client_id,audio_file_name)
    return data

@app.route('/open_ai_transcribe_audio_text', methods=['GET'])
def open_ai_transcribe_audio_text():
    client_id = int(request.args.get('clientid'))
    audio_file_name = request.args.get('audio_file')
    # file = 'D:/Cogent_AI_Audio_Repo/DMV-85311-MU1/DMV-85311-MU11_Chunk_6.wav'
    # need to change these hardcoded values
    file = 'D:/Cogent_AI_Audio_Repo/DMV-85311-MU1/Outbound_FollowUpCall-Z1.wav'
    status, transcript = open_ai_transcribe_audio(file)
    if status == 'success':
        data = {"text": transcript, 'status':"200"}
        return jsonify(data), 200
    return_data = {"text": 'no transcript', 'status': "500"}
    return jsonify(return_data), 500



if __name__ == '__main__':
    # app.run(debug=True)
    app.run(threaded=True)
