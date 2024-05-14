import re
import requests
from collections import Counter

# Sample unstructured text
text = """
Here are some sample IPs: 8.8.8.8, 1.1.1.1, 8.8.8.8.
Check these IPs: 192.168.1.1, 8.8.8.8, 1.2.3.4.
"""

# Regular expression to match IP addresses
ip_pattern = re.compile(r'\b(?:\d{1,3}\.){3}\d{1,3}\b')

# Extract IP addresses
ip_addresses = ip_pattern.findall(text)

# Count occurrences of each IP address
ip_counts = Counter(ip_addresses)

# Function to get geolocation and ISP information
def get_ip_info(ip):
    response = requests.get(f"https://ipinfo.io/{ip}/json")
    data = response.json()
    country = data.get('country', 'N/A')
    city = data.get('city', 'N/A')
    org = data.get('org', 'N/A')
    # Extracting ASN from org
    asn = org.split()[-1] if ' ' in org else 'N/A'
    isp = ' '.join(org.split()[:-1]) if ' ' in org else org
    return country, city, isp, asn

# Collect and print the information
for ip, count in ip_counts.items():
    country, city, isp, asn = get_ip_info(ip)
    if count > 1:
        print(f"{count} {ip} {country} {city} {isp} {asn}")
    else:
        print(f"{ip} {country} {city} {isp} {asn}")
