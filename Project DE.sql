-- создание схемы ds
create schema DS;
set search_path to ds;

-- создание таблиц под загрузку данных из csv-файлов
create table ft_balance_f(
	on_date date,
	account_rk numeric,
	currency_rk numeric,
	balance_out float,
	primary key (on_date, account_rk)
);

create table ft_posting_f(
	id int,
	oper_date date,
	credit_account_rk numeric,
	debet_account_rk numeric,
	credit_amount float,
	debet_amount float,
	primary key (id, oper_date, credit_account_rk, debet_account_rk)
);

create table md_account_d(
	data_actual_date date,
	data_actual_end_date date not null,
	account_rk numeric,
	account_number varchar(20) not null,
	char_type varchar(1) not null,
	currency_rk numeric not null,
	currency_code varchar(3) not null,
	primary key(data_actual_date, account_rk)
);

create table md_currency_d(
	currency_rk numeric,
	data_actual_date date,
	data_actual_end_date date,
	currency_code varchar(3),
	code_iso_char varchar(3),
	primary key (currency_rk, data_actual_date)
);

create table md_exchange_rate_d(
	data_actual_date date,
	data_actual_end_date date,
	currency_rk numeric,
	reduced_cource float,
	code_iso_num varchar(3),
	primary key (data_actual_date, currency_rk)
);

create table md_ledger_account_s(
	chapter char(1),
	chapter_name varchar(16),
	section_number int,
	section_name varchar(22),
	subsection_name varchar(21),
	ledger1_account int,
	ledger1_account_name varchar(47),
	ledger_account int,
	ledger_account_name varchar(153),
	characteristic char(1),
	is_resident int,
	is_reserve int,
	is_reserved int,
	is_loan int,
	is_reserved_assets int,
	is_overdue int,
	is_interest int,
	pair_account varchar(5),
	start_date date,
	end_date date,
	is_rub_only int,
	min_term varchar(1),
	min_term_measure varchar(1),
	max_term varchar(1),
	max_term_measure varchar(1),
	ledger_acc_full_name_translit varchar(1),
	is_revaluation varchar(1),
	is_correct varchar(1),
	primary key(ledger_account, start_date)
);

--проверка данных в таблицах
select * from md_ledger_account_s;
select * from md_account_d ;
select * from ft_balance_f fbf ;
select * from ft_posting_f fpf ;
select * from md_currency_d;
select * from md_exchange_rate_d merd ;
--delete  from md_exchange_rate_d ;
--delete from ft_balance_f ;
--delete from ft_posting_f ;
--delete from md_account_d ;
--delete from md_currency_d ;
--delete from md_ledger_account_s ;

-- создание схемы logs
CREATE SCHEMA logs;
SET search_path TO logs;

--создание таблицы logs для логирования загрузки данных
create table logs(
	message varchar(100),
	date_start timestamp,
	tablename varchar(50),
	operation char(6),
	status varchar(7),
	client_addr varchar(20),
	client_hostname varchar(25)
);

-- проверка логов в таблице
select * from logs;
--delete from logs;
