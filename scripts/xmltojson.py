import os
import json
from typing import Dict, Any, List, Tuple
import xml.etree.ElementTree as ET
def xml_to_json(file_path: str) -> Dict[str, Any]:
    """
    Convert one Informatica parameter XML to the target JSON structure:
    {
      "folder_name": "...",
      "workflow_name": "...",
      "parameters": { name: value, ... }
    }
    Namespace-agnostic (works even if default ns changes).
    """
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        # Namespace-agnostic lookups
        folder = root.find('.//{*}folder')
        workflow = root.find('.//{*}workflow')
        if folder is None or workflow is None:
            raise ValueError("Missing <folder> or <workflow> element")
        params: Dict[str, str] = {}
        for p in workflow.findall('.//{*}parameter'):
            name = p.get('name')
            if not name:
                # skip nameless parameters
                continue
            params[name] = (p.text or '').strip()
        return {
            "folder_name": folder.get('name'),
            "workflow_name": workflow.get('name'),
            "parameters": params
        }
    except Exception as e:
        # Bubble up with file context
        raise RuntimeError(f"Failed to parse {file_path}: {e}") from e

def process_project_dir(input_dir: str, output_json_path: str) -> Tuple[int, List[str]]:
    """
    Read all *.xml in `input_dir`, convert, and write a list JSON to `output_json_path`.
    Returns (count_written, errors).
    """
    results: List[Dict[str, Any]] = []
    errors: List[str] = []
    for name in sorted(os.listdir(input_dir)):
        if not name.lower().endswith(".xml"):
            continue
        file_path = os.path.join(input_dir, name)
        if not os.path.isfile(file_path):
            continue
        try:
            results.append(xml_to_json(file_path))
        except Exception as e:
            errors.append(str(e))
    # Write JSON even if empty, so you can see missing/empty folders
    os.makedirs(os.path.dirname(output_json_path), exist_ok=True)
    with open(output_json_path, "w", encoding="utf-8") as out:
        json.dump(results, out, indent=2, ensure_ascii=False, sort_keys=True)
    return len(results), errors

def process_root(root_xml_dir: str, out_root_dir: str) -> None:
    """
    For each immediate subfolder of `root_xml_dir`, produce `out_root_dir/<subfolder>.json`
    containing the array of parsed workflows from that subfolder.
    Example:
      root_xml_dir = /.../xml_params
        ├─ miniboss/  -> out_root_dir/miniboss.json
        ├─ something/ -> out_root_dir/something.json
    """
    os.makedirs(out_root_dir, exist_ok=True)
    for entry in sorted(os.scandir(root_xml_dir), key=lambda e: e.name.lower()):
        if not entry.is_dir():
            # Skip files in the root; only process subfolders
            continue
        subfolder = entry.name
        src_dir = entry.path
        dst_json = os.path.join(out_root_dir, f"{subfolder}.json")
        count, errors = process_project_dir(src_dir, dst_json)
        print(f"[OK] {subfolder}: wrote {count} records -> {dst_json}")
        if errors:
            print(f"[WARN] {subfolder}: {len(errors)} file(s) failed:")
            for msg in errors:
                print(f"    - {msg}")

if __name__ == "__main__":
    # Example usage with your paths:
    ROOT_XML_DIR = "/home/informaticaadmin/nfs_backup/codebase/xml_params"
    OUT_ROOT_DIR = "/home/informaticaadmin/nfs_backup/codebase/json_params"
    process_root(ROOT_XML_DIR, OUT_ROOT_DIR)
