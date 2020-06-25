import logging
from datetime import datetime, timezone

from faker import Faker
from utils.db_helper import get_redshift_customer_user_names
from utils.s3_helper import upload_file_to_s3, write_to_file

log = logging.getLogger()


def generate_web_log(seed_str: str):
    seed = datetime.strptime(seed_str, '%Y-%m-%dT%H:%M:%S+00:00')

    seed_int = int(seed.strftime('%Y%m%d%H%M%S'))
    Faker.seed(seed_int)
    fake: Faker = Faker()

    log.info(msg=f'seed value: {seed_int}')
    start_time = datetime(seed.year, seed.month, seed.day, 0, 0, 0)
    end_time = datetime(seed.year, seed.month, seed.day, 23, 59, 59)
    tz = timezone.utc

    user_names = get_redshift_customer_user_names()
    user_max = len(user_names) - 1

    http_status_code = [100, 200, 201, 204, 304, 400, 401, 403, 404, 409, 500]
    http_code_max = len(http_status_code) - 1

    res = []
    for _ in range(2000):
        ip = fake.ipv4_public()
        network_name = '-'
        user_name = user_names[fake.pyint(max_value=user_max)]
        time = fake.date_time_between_dates(datetime_start=start_time, datetime_end=end_time, tzinfo=tz).strftime(
            '[%d/%b/%Y%H:%M:%S %z]')
        request = '\"' + fake.http_method() + ' /' + fake.uri_path() + ' HTTP/1.0' + '\"'
        code = str(http_status_code[fake.pyint(max_value=http_code_max)])
        size = str(fake.pyint())
        referer = '\"' + fake.uri() + '\"'
        user_agent = '\"' + fake.user_agent() + '\"'
        res.append(str.join(' ', [ip, network_name, user_name, time, request, code, size, referer, user_agent]))

    time_str = seed.strftime('%Y-%m-%d')
    tmp_dir = f'/tmp/data/raw/{time_str}/'
    tmp_file_name = 'weblogs.log'
    write_to_file(str.join('\n', res), tmp_dir, tmp_file_name)

    s3_key = f'data/raw/{time_str}/weblogs.log'
    upload_file_to_s3(bucket_name='com.comp.prod.data.etl', key=s3_key, file=tmp_dir + tmp_file_name)
