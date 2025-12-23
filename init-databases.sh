#!/bin/bash
set -e
set -u

function create_user_and_database() {
    local database=$1
    local user=$2
    local password=$3
    
    echo "Создание пользователя '$user' и базы данных '$database'"
    
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        CREATE USER $user WITH PASSWORD '$password';
        CREATE DATABASE $database;
        GRANT ALL PRIVILEGES ON DATABASE $database TO $user;
EOSQL
}

# Создаем базу airflow и пользователя
if [ -n "$POSTGRES_MULTIPLE_DATABASES" ] && [ -n "$POSTGRES_MULTIPLE_USERS" ] && [ -n "$POSTGRES_MULTIPLE_PASSWORDS" ]; then
    IFS=',' read -r -a databases <<< "$POSTGRES_MULTIPLE_DATABASES"
    IFS=',' read -r -a users <<< "$POSTGRES_MULTIPLE_USERS"
    IFS=',' read -r -a passwords <<< "$POSTGRES_MULTIPLE_PASSWORDS"
    
    for i in "${!databases[@]}"; do
        if [ $i -eq 0 ]; then
            continue  # Первая база (akv_itsm) уже создана
        fi
        create_user_and_database "${databases[$i]}" "${users[$i-1]}" "${passwords[$i-1]}"
    done
fi
