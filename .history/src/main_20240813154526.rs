use pyo3::prelude::*;
use pyo3::types::PyModule;

fn main() -> PyResult<()> {
    // 初始化Python解释器
    Python::with_gil(|py| {
        // 定义Python代码
        let code = r#"
import pandas as pd
import requests
import pyarrow.parquet as pq
import pyarrow as pa
import re

# 下载日志数据
url = "https://raw.githubusercontent.com/elastic/examples/master/Common%20Data%20Formats/nginx_logs/nginx_logs"
response = requests.get(url)
log_data = response.text

# 解析日志数据的正则表达式模式
log_pattern = re.compile(
    r'(?P<ip>\S+) '  # IP地址
    r'\\S+ \\S+ '      # 忽略的字段
    r'\\[(?P<date>[^\\]]+)\\] '  # 日期
    r'"(?P<method>\\S+) '      # 请求方法
    r'(?P<url>\\S+) '          # URL
    r'(?P<proto>\\S+)" '       # 协议
    r'(?P<status>\\d+) '       # 状态码
    r'(?P<bytes>\\d+) '        # 字节数
    r'"(?P<referer>[^"]*)" '  # 引用页面
    r'"(?P<ua>[^"]*)"'        # 用户代理
)

# 使用正则表达式解析日志数据
logs = []
for line in log_data.splitlines():
    match = log_pattern.match(line)
    if match:
        log_dict = match.groupdict()
        # 转换status和bytes为整数
        log_dict['status'] = int(log_dict['status'])
        log_dict['bytes'] = int(log_dict['bytes'])
        logs.append(log_dict)

# 转换为Pandas DataFrame
df = pd.DataFrame(logs)

# 将date字段转换为日期时间格式
df['date'] = pd.to_datetime(df['date'], format='%d/%b/%Y:%H:%M:%S %z')

# 打印数据的基本信息
print(df.info())

# 转换为Parquet格式并保存
table = pa.Table.from_pandas(df)
pq.write_table(table, 'nginx_logs.parquet')

# 验证保存的Parquet文件
loaded_df = pq.read_table('nginx_logs.parquet').to_pandas()
print(loaded_df['ip'].describe())
"#;

        // 执行Python代码
        let _ = PyModule::new(py, "my_module")?.exec(code, None)?;

        Ok(())
    })
}