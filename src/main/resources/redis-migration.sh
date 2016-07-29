#!/bin/bash
source /etc/profile
java -cp redis-migration-1.0-SNAPSHOT.jar com.githoo.tool.redismigration.migration.RedisMigrationManager
