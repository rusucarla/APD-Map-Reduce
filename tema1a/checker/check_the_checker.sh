#!/bin/bash

CHECKER_SCRIPT="./checker.sh"  
OUTPUT_DIR="./output_files"   

mkdir -p "$OUTPUT_DIR"

for i in $(seq 1 100); do
    echo "checker: runda $i"
    OUTPUT_FILE="$OUTPUT_DIR/output_$i.txt"

    bash "$CHECKER_SCRIPT" > "$OUTPUT_FILE" 2>&1

    if [ $? -eq 0 ]; then
        echo "Runda $i completa, output salvat in $OUTPUT_FILE"
    else
        echo "Eroare la runda $i, verifica $OUTPUT_FILE pentru detalii"
    fi
done

echo "Toate rundele de checker au fost finalizate. Output-urile sunt in $OUTPUT_DIR"
