
def check_list(in_list, expected_size_of_list, obj_assert, err_msg):
    obj_assert.assertEqual(
        isinstance(in_list, list),
        True,
        "'in_list' is not a list."
    )

    obj_assert.assertEqual(
        len(in_list),
        expected_size_of_list,
        err_msg
    )

def get_sf_account_id(cs_api, cs_account_id, primary_storage_id, obj_assert, err_msg):
    sf_account_id_request = {'accountid': cs_account_id, 'storageid': primary_storage_id}
    sf_account_id_result = cs_api.getSolidFireAccountId(sf_account_id_request)
    sf_account_id = sf_account_id_result['apisolidfireaccountid']['solidFireAccountId']

    obj_assert.assertEqual(
        isinstance(sf_account_id, int),
        True,
        err_msg
    )

    return sf_account_id

def get_iqn(cs_api, volume, obj_assert):
    # Get volume IQN
    sf_iscsi_name_request = {'volumeid': volume.id}
    sf_iscsi_name_result = cs_api.getVolumeiScsiName(sf_iscsi_name_request)
    sf_iscsi_name = sf_iscsi_name_result['apivolumeiscsiname']['volumeiScsiName']

    check_iscsi_name(sf_iscsi_name, obj_assert)

    return sf_iscsi_name

def check_iscsi_name(sf_iscsi_name, obj_assert):
    obj_assert.assertEqual(
        sf_iscsi_name[0],
        "/",
        "The iSCSI name needs to start with a forward slash."
    )

def set_supports_resign(supports_resign, db_connection):
    _set_supports_resign_for_table(supports_resign, db_connection, "host_details")
    _set_supports_resign_for_table(supports_resign, db_connection, "cluster_details")

def _set_supports_resign_for_table(supports_resign, db_connection, table):
    sql_query = "Update " + str(table) + " Set value = '" + str(supports_resign) + "' Where name = 'supportsResign'"

    # make sure you can connect to MySQL: https://teamtreehouse.com/community/cant-connect-remotely-to-mysql-server-with-mysql-workbench
    db_connection.execute(sql_query)

def purge_solidfire_volumes(sf_client):
    deleted_volumes = sf_client.list_deleted_volumes()

    for deleted_volume in deleted_volumes:
        sf_client.purge_deleted_volume(deleted_volume['volumeID'])

def get_active_sf_volumes(sf_client, sf_account_id=None):
    if sf_account_id is not None:
        sf_volumes = sf_client.list_volumes_for_account(sf_account_id)

        if sf_volumes is not None and len(sf_volumes) > 0:
            sf_volumes = _get_active_sf_volumes_only(sf_volumes)
    else:
        sf_volumes = sf_client.list_active_volumes()

    return sf_volumes

def _get_active_sf_volumes_only(sf_volumes):
    active_sf_volumes_only = []

    for sf_volume in sf_volumes:
        if sf_volume["status"] == "active":
            active_sf_volumes_only.append(sf_volume)

    return active_sf_volumes_only
