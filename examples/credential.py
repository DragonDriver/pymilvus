from pymilvus import utility, connections

_COLLECTION = "demo"
_HOST = '127.0.0.1'
_PORT = '19530'
_CONNECTION_NAME = "default"
_ANOTHER_CONNECTION_NAME = "another_conn"
_USER = "user"
_PASSWORD = "password"
_ANOTHER_USER = "another_user"
_ANOTHER_PASSWORD = "another_password"
_NEW_PASSWORD = "new_password"

# connect to Milvus.
connections.connect(alias=_CONNECTION_NAME,
                    host=_HOST,
                    port=_PORT,
                    user=_USER,
                    password=_PASSWORD)

# test if connection is legal.
has = utility.has_collection(_COLLECTION, using=_CONNECTION_NAME)
print(f"has collection {_COLLECTION}: {has}")
users = utility.list_cred_users(using=_CONNECTION_NAME)
print(f"users in Milvus: {users}")

# create another credential.
utility.create_credential(_ANOTHER_USER, _ANOTHER_PASSWORD, using=_CONNECTION_NAME)

# update credential.
utility.create_credential(_ANOTHER_USER, _NEW_PASSWORD, using=_CONNECTION_NAME)

# establish a new connection using the created credential.
connections.connect(alias=_ANOTHER_CONNECTION_NAME,
                    host=_HOST,
                    port=_PORT,
                    user=_ANOTHER_USER,
                    password=_NEW_PASSWORD)

# test if newly-created connection is legal.
has = utility.has_collection(_COLLECTION, using=_ANOTHER_CONNECTION_NAME)
print(f"has collection {_COLLECTION}: {has}")
users = utility.list_cred_users(using=_ANOTHER_CONNECTION_NAME)
print(f"users in Milvus: {users}")
print(f"{_ANOTHER_USER} in users: {_ANOTHER_USER in users}")

# delete credential.
utility.delete_credential(_ANOTHER_USER, using=_CONNECTION_NAME)
users = utility.list_cred_users(using=_ANOTHER_CONNECTION_NAME)
print(f"users in Milvus: {users}")
print(f"{_ANOTHER_USER} in users: {_ANOTHER_USER in users}")
