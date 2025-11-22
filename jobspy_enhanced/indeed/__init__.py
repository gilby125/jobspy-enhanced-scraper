from __future__ import annotations

import json
import math
from datetime import datetime
from typing import Tuple

from jobspy_enhanced.indeed.constant import job_search_query, api_headers
from jobspy_enhanced.indeed.util import is_job_remote, get_compensation, get_job_type
from jobspy_enhanced.model import (
    Scraper,
    ScraperInput,
    Site,
    JobPost,
    Location,
    JobResponse,
    JobType,
    DescriptionFormat,
)
from jobspy_enhanced.util import (
    extract_emails_from_text,
    markdown_converter,
    create_session,
    create_logger,
)

log = create_logger("Indeed")


class Indeed(Scraper):
    def __init__(
        self, proxies: list[str] | str | None = None, ca_cert: str | None = None, user_agent: str | None = None
    ):
        """
        Initializes IndeedScraper with the Indeed API url
        """
        super().__init__(Site.INDEED, proxies=proxies)

        self.session = create_session(
            proxies=self.proxies, ca_cert=ca_cert, is_tls=False
        )
        self.scraper_input = None
        self.jobs_per_page = 100
        self.num_workers = 10
        self.seen_urls = set()
        self.headers = None
        self.api_country_code = None
        self.base_url = None
        self.api_url = "https://apis.indeed.com/graphql"

    def scrape(self, scraper_input: ScraperInput) -> JobResponse:
        """
        Scrapes Indeed for jobs with scraper_input criteria
        :param scraper_input:
        :return: job_response
        """
        self.scraper_input = scraper_input
        domain, self.api_country_code = self.scraper_input.country.indeed_domain_value
        self.base_url = f"https://{domain}.indeed.com"
        self.headers = api_headers.copy()
        self.headers["indeed-co"] = self.scraper_input.country.indeed_domain_value
        job_list = []
        page = 1

        cursor = None

        while len(self.seen_urls) < scraper_input.results_wanted + scraper_input.offset:
            log.info(
                f"search page: {page} / {math.ceil(scraper_input.results_wanted / self.jobs_per_page)}"
            )
            jobs, cursor = self._scrape_page(cursor)
            if not jobs:
                log.info(f"found no jobs on page: {page}")
                break
            job_list += jobs
            page += 1
        return JobResponse(
            jobs=job_list[
                scraper_input.offset : scraper_input.offset
                + scraper_input.results_wanted
            ]
        )

    def _scrape_page(self, cursor: str | None) -> Tuple[list[JobPost], str | None]:
        """
        Scrapes a page of Indeed for jobs with scraper_input criteria
        :param cursor:
        :return: jobs found on page, next page cursor
        """
        jobs = []
        new_cursor = None
        filters = self._build_filters()
        search_term = (
            self.scraper_input.search_term.replace('"', '\\"')
            if self.scraper_input.search_term
            else ""
        )
        # Build location parameter
        # Indeed API requires radius and radiusUnit when using location parameter
        # Use a large default radius (10000 miles) for country-wide searches when distance is None
        location_str = ""
        if self.scraper_input.location:
            distance = self.scraper_input.distance if self.scraper_input.distance is not None else 10000
            location_str = f'location: {{where: "{self.scraper_input.location}", radius: {distance}, radiusUnit: MILES}}'
        
        query = job_search_query.format(
            what=(f'what: "{search_term}"' if search_term else ""),
            location=location_str,
            dateOnIndeed=self.scraper_input.hours_old,
            cursor=f'cursor: "{cursor}"' if cursor else "",
            filters=filters,
        )
        payload = {
            "query": query,
        }
        api_headers_temp = api_headers.copy()
        api_headers_temp["indeed-co"] = self.api_country_code
        response = self.session.post(
            self.api_url,
            headers=api_headers_temp,
            json=payload,
            timeout=10,
            verify=False,
        )
        if not response.ok:
            error_msg = f"responded with status code: {response.status_code}"
            try:
                error_data = response.json()
                if "errors" in error_data:
                    error_msg += f" - Errors: {error_data['errors']}"
                elif "message" in error_data:
                    error_msg += f" - Message: {error_data['message']}"
            except:
                error_msg += f" - Response: {response.text[:200]}"
            log.info(f"{error_msg} (submit GitHub issue if this appears to be a bug)")
            return jobs, new_cursor
        data = response.json()
        jobs = data["data"]["jobSearch"]["results"]
        new_cursor = data["data"]["jobSearch"]["pageInfo"]["nextCursor"]

        job_list = []
        for job in jobs:
            processed_job = self._process_job(job["job"])
            if processed_job:
                job_list.append(processed_job)

        return job_list, new_cursor

    def _build_filters(self):
        """
        Builds the filters dict for job type/is_remote with support for combining multiple filter types.
        IndeedApply: filters: { keyword: { field: "indeedApplyScope", keys: ["DESKTOP"] } }
        """
        filters = []
        
        # Add date filter if hours_old is specified
        if self.scraper_input.hours_old:
            filters.append({
                "date": {
                    "field": "dateOnIndeed",
                    "start": f"{self.scraper_input.hours_old}h"
                }
            })
        
        # Add easy apply filter if specified
        if self.scraper_input.easy_apply:
            filters.append({
                "keyword": {
                    "field": "indeedApplyScope",
                    "keys": ["DESKTOP"]
                }
            })
        
        # Add job type and remote filters
        if self.scraper_input.job_type or self.scraper_input.is_remote:
            job_type_key_mapping = {
                JobType.FULL_TIME: "CF3CP",
                JobType.PART_TIME: "75GKK",
                JobType.CONTRACT: "NJXCK",
                JobType.INTERNSHIP: "VDTG7",
            }

            keys = []
            if self.scraper_input.job_type:
                key = job_type_key_mapping[self.scraper_input.job_type]
                keys.append(key)

            if self.scraper_input.is_remote:
                keys.append("DSQF7")

            if keys:
                filters.append({
                    "keyword": {
                        "field": "attributes",
                        "keys": keys
                    }
                })
        
        # Build the final filters string
        if filters:
            if len(filters) == 1:
                # Single filter - use direct format
                filter_content = self._format_single_filter(filters[0])
                filters_str = f"filters: {{{filter_content}}}"
            else:
                # Multiple filters - use composite format
                filter_contents = [self._format_single_filter(f) for f in filters]
                filters_str = f"""
                filters: {{
                  composite: {{
                    filters: [{', '.join(filter_contents)}]
                  }}
                }}
                """
        else:
            filters_str = ""
            
        return filters_str
    
    def _format_single_filter(self, filter_dict):
        """Helper method to format a single filter for GraphQL"""
        if "date" in filter_dict:
            date_filter = filter_dict["date"]
            return f'date: {{ field: "{date_filter["field"]}", start: "{date_filter["start"]}" }}'
        elif "keyword" in filter_dict:
            keyword_filter = filter_dict["keyword"]
            keys_str = '", "'.join(keyword_filter["keys"])
            return f'keyword: {{ field: "{keyword_filter["field"]}", keys: ["{keys_str}"] }}'
        return ""

    def _process_job(self, job: dict) -> JobPost | None:
        """
        Parses the job dict into JobPost model
        :param job: dict to parse
        :return: JobPost if it's a new job
        """
        job_url = f'{self.base_url}/viewjob?jk={job["key"]}'
        if job_url in self.seen_urls:
            return
        self.seen_urls.add(job_url)
        description = job["description"]["html"]
        if self.scraper_input.description_format == DescriptionFormat.MARKDOWN:
            description = markdown_converter(description)

        job_type = get_job_type(job["attributes"])
        timestamp_seconds = job["datePublished"] / 1000
        date_posted = datetime.fromtimestamp(timestamp_seconds).strftime("%Y-%m-%d")
        employer = job["employer"].get("dossier") if job["employer"] else None
        employer_details = employer.get("employerDetails", {}) if employer else {}
        rel_url = job["employer"]["relativeCompanyPageUrl"] if job["employer"] else None
        return JobPost(
            id=f'in-{job["key"]}',
            title=job["title"],
            description=description,
            company_name=job["employer"].get("name") if job.get("employer") else None,
            company_url=(f"{self.base_url}{rel_url}" if job["employer"] else None),
            company_url_direct=(
                employer["links"]["corporateWebsite"] if employer else None
            ),
            location=Location(
                city=job.get("location", {}).get("city"),
                state=job.get("location", {}).get("admin1Code"),
                country=job.get("location", {}).get("countryCode"),
            ),
            job_type=job_type,
            compensation=get_compensation(job["compensation"]),
            date_posted=date_posted,
            job_url=job_url,
            job_url_direct=(
                job["recruit"].get("viewJobUrl") if job.get("recruit") else None
            ),
            emails=extract_emails_from_text(description) if description else None,
            is_remote=is_job_remote(job, description),
            company_addresses=(
                employer_details["addresses"][0]
                if employer_details.get("addresses")
                else None
            ),
            company_industry=(
                employer_details["industry"]
                .replace("Iv1", "")
                .replace("_", " ")
                .title()
                .strip()
                if employer_details.get("industry")
                else None
            ),
            company_num_employees=employer_details.get("employeesLocalizedLabel"),
            company_revenue=employer_details.get("revenueLocalizedLabel"),
            company_description=employer_details.get("briefDescription"),
            company_logo=(
                employer["images"].get("squareLogoUrl")
                if employer and employer.get("images")
                else None
            ),
        )

    def format_job_for_display(self, job: JobPost) -> dict:
        """Format a job posting for readable output"""
        output = {
            "Title": job.title,
            "Company": job.company_name,
            "Location": job.location.display_location() if job.location else "N/A",
            "Is Remote": "Yes" if job.is_remote else "No",
            "Job Type": ", ".join([jt.value[0] for jt in job.job_type]) if job.job_type else "N/A",
            "Date Posted": job.date_posted,
            "Job URL (Indeed)": job.job_url,
            "Direct Apply URL": job.job_url_direct or "Not available",
            "Company URL": job.company_url or "Not available",
            "Company Direct URL": job.company_url_direct or "Not available",
        }
        
        # Add compensation if available
        if job.compensation:
            comp = job.compensation
            salary_str = ""
            if comp.min_amount and comp.max_amount:
                salary_str = f"${comp.min_amount:,.0f} - ${comp.max_amount:,.0f}"
            elif comp.min_amount:
                salary_str = f"${comp.min_amount:,.0f}+"
            elif comp.max_amount:
                salary_str = f"Up to ${comp.max_amount:,.0f}"
            
            if salary_str:
                output["Salary"] = f"{salary_str} {comp.currency} ({comp.interval.value if comp.interval else 'N/A'})"
            else:
                output["Salary"] = "Not specified"
        else:
            output["Salary"] = "Not specified"
        
        # Add company details if available
        if job.company_industry:
            output["Company Industry"] = job.company_industry
        if job.company_num_employees:
            output["Company Employees"] = job.company_num_employees
        if job.company_revenue:
            output["Company Revenue"] = job.company_revenue
        
        # Add description preview (first 200 chars)
        if job.description:
            desc_preview = job.description.replace('\n', ' ').strip()[:200]
            output["Description Preview"] = desc_preview + "..." if len(job.description) > 200 else desc_preview
        
        return output

    def format_jobs_for_json(self, jobs: list[JobPost]) -> list[dict]:
        """Format jobs for JSON export with proper serialization"""
        jobs_data = []
        for job in jobs:
            job_dict = job.model_dump()
            # Convert location object to dict for better readability
            if job.location:
                # Handle country - it can be a Country enum, string, or None
                country_value = None
                if job.location.country:
                    if isinstance(job.location.country, str):
                        country_value = job.location.country
                    else:
                        # It's a Country enum
                        country_value = job.location.country.value[0]
                
                job_dict['location'] = {
                    'city': job.location.city,
                    'state': job.location.state,
                    'country': country_value,
                    'display': job.location.display_location()
                }
            # Convert job_type list to list of strings for better readability
            if job.job_type:
                job_dict['job_type'] = [jt.value[0] for jt in job.job_type]
            jobs_data.append(job_dict)
        return jobs_data

    def get_summary_statistics(self, jobs: list[JobPost]) -> dict:
        """Generate summary statistics for the job results"""
        if not jobs:
            return {
                "total_jobs": 0,
                "jobs_with_direct_apply": 0,
                "jobs_with_direct_apply_percent": 0.0,
                "remote_jobs": 0,
                "remote_jobs_percent": 0.0,
                "jobs_with_salary": 0,
                "jobs_with_salary_percent": 0.0,
            }
        
        total = len(jobs)
        jobs_with_direct_apply = sum(1 for job in jobs if job.job_url_direct)
        remote_jobs = sum(1 for job in jobs if job.is_remote)
        jobs_with_salary = sum(1 for job in jobs if job.compensation and (job.compensation.min_amount or job.compensation.max_amount))
        
        return {
            "total_jobs": total,
            "jobs_with_direct_apply": jobs_with_direct_apply,
            "jobs_with_direct_apply_percent": (jobs_with_direct_apply / total * 100) if total > 0 else 0.0,
            "remote_jobs": remote_jobs,
            "remote_jobs_percent": (remote_jobs / total * 100) if total > 0 else 0.0,
            "jobs_with_salary": jobs_with_salary,
            "jobs_with_salary_percent": (jobs_with_salary / total * 100) if total > 0 else 0.0,
        }

    def format_direct_apply_urls(self, jobs: list[JobPost], title: str = "Direct Apply URLs") -> str:
        """Format direct apply URLs for text file export"""
        lines = [f"{title}\n", "=" * 80 + "\n\n"]
        for idx, job in enumerate(jobs, 1):
            if job.job_url_direct:
                lines.append(f"{idx}. {job.title} - {job.company_name}\n")
                lines.append(f"   {job.job_url_direct}\n\n")
            else:
                lines.append(f"{idx}. {job.title} - {job.company_name}\n")
                lines.append(f"   (No direct apply URL - use Indeed URL: {job.job_url})\n\n")
        return "".join(lines)

    def display_results(self, jobs: list[JobPost], search_term: str = None) -> None:
        """Display formatted job results to console"""
        if not jobs:
            print("No jobs found matching the criteria.")
            return
        
        # Display each job
        for idx, job in enumerate(jobs, 1):
            print(f"\n{'='*80}")
            print(f"Job #{idx}")
            print(f"{'='*80}")
            
            job_output = self.format_job_for_display(job)
            for key, value in job_output.items():
                print(f"{key:25}: {value}")
            
            # Highlight direct apply URL
            if job.job_url_direct:
                print(f"\n{'='*40}")
                print(f"DIRECT APPLY URL (redirects to company portal):")
                print(f"{job.job_url_direct}")
                print(f"{'='*40}")
            else:
                print(f"\n[WARNING] No direct apply URL available. Use Indeed URL: {job.job_url}")
        
        # Display summary statistics
        stats = self.get_summary_statistics(jobs)
        print(f"\n\n{'='*80}")
        print("SUMMARY STATISTICS")
        print(f"{'='*80}")
        print(f"Total Jobs Found: {stats['total_jobs']}")
        print(f"Jobs with Direct Apply URL: {stats['jobs_with_direct_apply']} ({stats['jobs_with_direct_apply_percent']:.1f}%)")
        print(f"Remote Jobs: {stats['remote_jobs']} ({stats['remote_jobs_percent']:.1f}%)")
        print(f"Jobs with Salary Information: {stats['jobs_with_salary']} ({stats['jobs_with_salary_percent']:.1f}%)")

    def save_results(self, jobs: list[JobPost], output_prefix: str = "indeed_jobs") -> dict[str, str]:
        """
        Save job results to JSON and direct apply URLs to text file
        Returns a dict with 'json_file' and 'txt_file' keys containing the file paths
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        json_file = f"{output_prefix}_{timestamp}.json"
        txt_file = f"{output_prefix}_direct_apply_urls_{timestamp}.txt"
        
        # Save JSON
        jobs_data = self.format_jobs_for_json(jobs)
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(jobs_data, f, indent=2, default=str, ensure_ascii=False)
        
        # Save direct apply URLs
        direct_apply_content = self.format_direct_apply_urls(
            jobs,
            title=f"Direct Apply URLs ({len(jobs)} jobs)"
        )
        with open(txt_file, 'w', encoding='utf-8') as f:
            f.write(direct_apply_content)
        
        return {
            'json_file': json_file,
            'txt_file': txt_file
        }
