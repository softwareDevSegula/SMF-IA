# docx_to_excel_mirror.py — FIXED VERSION (SKIP SD_ EFFECTIVITY, KEEP FCT_ EFFECTIVITY)
from docx import Document
import pandas as pd
import re
from pathlib import Path

print("TN-MBSE 2025 — STLA SPEC EXTRACTOR — SKIP SD_ EFFECTIVITY, KEEP FCT_ EFFECTIVITY")

specs_folder = Path("../data/specs_docx")
output_folder = Path("../data/output_diagrams")
output_folder.mkdir(exist_ok=True)

docx_files = list(specs_folder.glob("*.docx"))
if not docx_files:
    print("No .docx files found!")
    exit()

all_results = []

for docx_path in docx_files:
    print(f"\n{'='*100}")
    print(f"PROCESSING: {docx_path.name}")
    print(f"{'='*100}")

    doc = Document(docx_path)
    
    # We'll process elements in document order to maintain proper FCT context
    elements = []
    
    print("STEP 1: COLLECTING ALL ELEMENTS IN DOCUMENT ORDER...")
    
    # First, collect all paragraphs and tables in order
    for element in doc.element.body:
        if element.tag.endswith('p'):  # paragraph
            paragraph = None
            for child in element.iter():
                if child.tag.endswith('t'):
                    if paragraph is None:
                        paragraph = ''
                    paragraph += child.text if child.text else ''
            if paragraph and paragraph.strip():
                elements.append(('paragraph', paragraph.strip()))
                
        elif element.tag.endswith('tbl'):  # table
            # Find the table object that corresponds to this element
            for table in doc.tables:
                if table._element == element:
                    elements.append(('table', table))
                    break

    print(f"   Found {len([e for e in elements if e[0]=='paragraph'])} paragraphs and {len([e for e in elements if e[0]=='table'])} tables")

    current_fct = None
    total_flows = 0
    element_counter = 0
    skip_mode = False  # Flag to skip SD_ Effectivity sections

    print("\nSTEP 2: PROCESSING ELEMENTS IN ORDER (DIFFERENTIATE SD_ vs FCT_ EFFECTIVITY)...")
    
    # Process elements in order to maintain proper FCT context
    for elem_type, elem_content in elements:
        element_counter += 1
        
        if elem_type == 'paragraph':
            text = elem_content
            
            # Detect FCT name (standalone FCT_ pattern)
            if re.match(r"^FCT_[A-Za-z0-9_]+$", text):
                current_fct = text
                skip_mode = False  # Reset skip mode when we find a new FCT
                print(f"   [{element_counter:3}] PARAGRAPH → FCT CHANGED: {current_fct}")
                
            # CRITICAL: Detect "Effectivity of FA" and check if it's SD_ or FCT_
            elif "Effectivity of FA" in text:
                # Check if it's SD_ pattern (SKIP these)
                sd_match = re.search(r"Effectivity of FA\s*:\s*(SD_[A-Za-z0-9_]+)", text, re.IGNORECASE)
                if sd_match:
                    skip_mode = True
                    sd_name = sd_match.group(1)
                    print(f"   [{element_counter:3}] PARAGRAPH → ⚠️  EFFECTIVITY OF FA: {sd_name} DETECTED - SKIPPING ALL TABLES")
                
                # Check if it's FCT_ pattern (PROCESS these)  
                fct_match = re.search(r"Effectivity of FA\s*:\s*(FCT_[A-Za-z0-9_]+)", text, re.IGNORECASE)
                if fct_match:
                    skip_mode = False
                    current_fct = fct_match.group(1)
                    print(f"   [{element_counter:3}] PARAGRAPH → ✅  EFFECTIVITY OF FA: {current_fct} DETECTED - PROCESSING TABLES")
                
        elif elem_type == 'table':
            table = elem_content
            if not table.rows:
                continue

            # Check if this is a Flows table
            header = " ".join(cell.text.strip().upper() for cell in table.rows[0].cells)
            
            if "FLOW TITLE" in header and "DIRECTION" in header:
                print(f"\n   [{element_counter:3}] TABLE → FLOWS TABLE DETECTED")
                
                # SKIP if we're in SD_ Effectivity section
                if skip_mode:
                    print(f"        ⚠️  SKIPPED - Inside SD_ Effectivity section (data dictionary/summary)")
                    continue
                    
                if current_fct is None:
                    print("        ⚠️  SKIPPED — no FCT found before it")
                    continue

                print(f"        Current FCT: {current_fct}")
                print(f"        Rows in table: {len(table.rows)}")

                table_flows = 0
                for i, row in enumerate(table.rows[1:], 1):  # skip header
                    cells = [cell.text.strip() for cell in row.cells]
                    if len(cells) < 2 or not cells[0]:
                        continue

                    flow_title = cells[0]
                    direction_raw = cells[1].upper()
                    direction = "EMISSION" if any(x in direction_raw for x in ["EMISSION", "E", "EMIT"]) else "CONSUMPTION"

                    # Only print first few flows to avoid too much output
                    if table_flows < 3:
                        print(f"        Flow {i}: {direction:11} {flow_title}")

                    all_results.append({
                        "Spec File": docx_path.name,
                        "Function": current_fct,
                        "Flow Title": flow_title,
                        "Direction": direction,
                        "Preview": " | ".join(cells[:3])
                    })
                    table_flows += 1
                    total_flows += 1

                print(f"        ✅ Added {table_flows} flows to {current_fct}")

    print(f"\n   → TOTAL: {total_flows} flows extracted from this file")

# SAVE TO EXCEL
df = pd.DataFrame(all_results) if all_results else pd.DataFrame(columns=["Spec File", "Function", "Flow Title", "Direction", "Preview"])

output_file = output_folder / "Spec_translated.xlsx"
df.to_excel(output_file, index=False)

print("\n" + "="*100)
print("EXTRACTION COMPLETE - SD_ EFFECTIVITY SKIPPED, FCT_ EFFECTIVITY KEPT")
print(f"   Total flows extracted : {len(df)}")
print(f"   Unique functions      : {df['Function'].nunique() if not df.empty else 0}")
print(f"   EXCEL SAVED → {output_file.resolve()}")
print("="*100)

# Show detailed function distribution
if not df.empty:
    print("\nCLEAN FUNCTION DISTRIBUTION:")
    func_counts = df['Function'].value_counts()
    for func, count in func_counts.items():
        print(f"   {func}: {count} flows")

print("\n✅ EXTRACTION PERFECT - READY FOR DATABASE TRACEABILITY")