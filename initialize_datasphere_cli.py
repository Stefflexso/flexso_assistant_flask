import subprocess

def set_host(host: str):
    """
    function for setting host
    runs a command
    parameters: 
        host: string of host value; example: "https://flexso-2.eu10.hcs.cloud.sap/"
    """

    command = (
        f"datasphere config host set {host}"
    )    
    subprocess.run(command, shell=True, capture_output=True, text=True, timeout=30)

def cache_initialization(host, json_path):
    """
    function for initializing cache with secrets file
    runs a command
    parameters:
        host: string of host value; example: "https://flexso-2.eu10.hcs.cloud.sap/"
        json_path: string of json path to the secrets file; example: "secrets_file.json"
    secrets file needs to contain a number of values, most important access token and refresh token
    example of secrets file:
        {
            "client_id": "sb-449a31a1-bff6-4a34-8741-be4aaf276005!b107066|client!b3650",
            "client_secret": "5bfa3bef-9fb4-4372-a8cc-7eef3a67oxsD8lxMOaLjiWC5YPlNPae3Fj7cBPEQdjgNAc=",
            "authorization_url": "https://flexso-2.authentication.eu10.hana.ondemand.com/oauth/authorize",
            "token_url": "https://flexso-2.authentication.eu10.hana.ondemand.com/oauth/token",
            "access_token": "eyJhbGciOiJSUzI1NieHNvLTIuYXV0aGVudGljYXRpb24uZXUxMC5oYW5hLm9uZGVtYW5kLmNvbS90b2tlbl9rZXlzIiwia2lkIjoiZGVmYXVsdC1qd3Qta2V5LTE5Mzg4NzM4MjIiLCJ0eXAiOiJKV1QiLCJqaWQiOiAiWWFKL0JUdFNZV3ZYY3lNN292YU50NUxkbkRGSUNvdXZIZ1J2S3ZnRGpkND0ifQ.eyJqdGkiOiIwZmJkNzBiN2I3MmM0YzRjYWFiMjdmOGJkYWRhZGYwMyIsImV4dF9hdHRyIjp7ImVuaGFuY2VyIjoiWFNVQUEiLCJzdWJhY2NvdW50aWQiOiI1ZTc1MzU1NC1kZjNjLTQ0ODMtOWUzOS0xOTA0YzRhOTBiM2QiLCJ6ZG4iOiJmbGV4c28tMiIsInNlcnZpY2VpbnN0YW5jZWlkIjoiNjVkNTM0ZDYtZTNiOS00ZjExLWFlMjUtNTdiZDk3MTM2YTc2In0sInVzZXJfdXVpZCI6ImQzOWI2MWRlLTIwODItNDQwYy1iNmMxLWU4MDZmYWIwNzU0NCIsInhzLnVzZXIuYXR0cmlidXRlcyI6e30sInhzLnN5c3RlbS5hdHRyaWJ1dGVzIjp7InhzLnNhbWwuZ3JvdXBzIjpbInNhYyJdLCJ4cy5yb2xlY29sbGVjdGlvbnMiOlsic2FjLnVzZXJzIl19LCJnaXZlbl9uYW1lIjoidmFuZ2VnMSIsImZhbWlseV9uYW1lIjoiY3Jvbm9zLmJlIiwic3ViIjoiNDgzMDVkMjktYWMwNy00ODI1LTgxNjAtYmQwYjVjNThlZTBhIiwic2NvcGUiOlsib3BlbmlkIiwiYXBwcm91dGVyLXNhYy1zYWNldTEwIXQzNjUwLnNhcC5mcGEudXNlciIsInVhYS51c2VyIl0sImNsaWVudF9pZCI6InNiLTQ0OWEzMWExLWJmZjYtNGEzNC04NzQxLWJlNGFhZjI3NjAwNSFiMTA3MDY2fGNsaWVudCFiMzY1MCIsImNpZCI6InNiLTQ0OWEzMWExLWJmZjYtNGEzNC04NzQxLWJlNGFhZjI3NjAwNSFiMTA3MDY2fGNsaWVudCFiMzY1MCIsImF6cCI6InNiLTQ0OWEzMWExLWJmZjYtNGEzNC04NzQxLWJlNGFhZjI3NjAwNSFiMTA3MDY2fGNsaWVudCFiMzY1MCIsImdyYW50X3R5cGUiOiJhdXRob3JpemF0aW9uX2NvZGUiLCJ1c2VyX2lkIjoiNDgzMDVkMjktYWMwNy00ODI1LTgxNjAtYmQwYjVjNThlZTBhIiwib3JpZ2luIjoiYTFic3kzcGVxLmFjY291bnRzLm9uZGVtYW5kLmNvbSIsInVzZXJfbmFtZSI6InZhbmdlZzFAY3Jvbm9zLmJlIiwiZW1haWwiOiJ2YW5nZWcxQGNyb25vcy5iZSIsImF1dGhfdGltZSI6MTc1NTYxMzI3NywicmV2X3NpZyI6Ijk0ODVhNjZjIiwiaWF0IjoxNzU1NjEzNDA4LCJleHAiOjE3NTU2MTcwMDgsImlzcyI6Imh0dHBzOi8vZmxleHNvLTIuYXV0aGVudGljYXRpb24uZXUxMC5oYW5hLm9uZGVtYW5kLmNvbS9vYXV0aC90b2tlbiIsInppZCI6IjVlNzUzNTU0LWRmM2MtNDQ4My05ZTM5LTE5MDRjNGE5MGIzZCIsImF1ZCI6WyJhcHByb3V0ZXItc2FjLXNhY2V1MTAhdDM2NTAuc2FwLmZwYSIsInVhYSIsIm9wZW5pZCIsInNiLTQ0OWEzMWExLWJmZjYtNGEzNC04NzQxLWJlNGFhZjI3NjAwNSFiMTA3MDY2fGNsaWVudCFiMzY1MCJdfQ.hwYJ1WlpdvhIctccSKW5IXwtfYCOGRvQYKSy0cWeUdgQhVM4-wAZQQ6oO_BOtqwnuA2q-5suX4Hg1-VPTR3pWIusw-w_LD-AeOsgKzOn66gxpN-g6OI4vrDowwOvLDRKobw-ucuoD-u938Wn8TTxIHNrDoiAt058CjvKwLBAVM7IbbZpBDO3xt-TavMeAo136UCWMwrMOE4LcIweoSnDY5ZY9s25i0quzxMaohbJKixKClnEurTDPnhWBMBn3D3j8TvItTZNZcfIIoV3KyyeDQGnGjCjcHSfrxkCZGJGLXVe9c5XaIv7C_gotaMQRwc6NvPu2HfBDbEY-mXP5ZNKBQ",
            "refresh_token": "544503bf5ebca161da-r",
            "host": "https://flexso-2.eu10.hcs.cloud.sap/"
        }
    """

    command = (
        f"datasphere config cache init --host {host} --secrets-file {json_path}"
    )    
    subprocess.run(command, shell=True, capture_output=True, text=True, timeout=30)
