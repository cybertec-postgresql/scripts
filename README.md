# scripts
Various scripts around PostgreSQL management

#### parallel_basebackup.sh

Compressed streaming basebackups over SSH, useful for slow networks.

#### quick_verify.py

Dumping of all tables to /dev/null in parallel without a snapshot. Meant for quick validation for new replicas.
