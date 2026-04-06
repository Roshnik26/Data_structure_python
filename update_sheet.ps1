$excel = New-Object -ComObject Excel.Application
$excel.Visible = $false
$excel.DisplayAlerts = $false
try {
    $workbook = $excel.Workbooks.Open("C:\Users\admin\OneDrive\Desktop\Data_structure_python\version 2 dataset.xlsx")
    $sheet = $workbook.Sheets.Item(1)

    $cols = $sheet.UsedRange.Columns.Count

    for ($i = 1; $i -le $cols; $i++) {
        $header = $sheet.Cells.Item(1, $i).Value2
        if ($header -eq $null) { continue }
        
        if ($header -eq "Dose_InVitro_Max_ugmL") {
            $sheet.Cells.Item(1, $i).Value2 = "Dose (mg)"
            Write-Host "Updating Dose column at index $i"
            $range = $sheet.Range($sheet.Cells.Item(2, $i), $sheet.Cells.Item($sheet.UsedRange.Rows.Count, $i))
            $vals = $range.Value2
            for ($r = 1; $r -le $vals.GetLength(0); $r++) {
                if ($vals[$r, 1] -is [double] -or $vals[$r, 1] -is [int]) {
                    $vals[$r, 1] = $vals[$r, 1] / 1000.0
                }
            }
            $range.Value2 = $vals
        }
        elseif ($header -match "IC50" -or $header -match "ic50") {
            if ($header -match "ug") {
                $sheet.Cells.Item(1, $i).Value2 = "IC50 (mg)"
                Write-Host "Updating IC50 column at index $i"
                $range = $sheet.Range($sheet.Cells.Item(2, $i), $sheet.Cells.Item($sheet.UsedRange.Rows.Count, $i))
                $vals = $range.Value2
                for ($r = 1; $r -le $vals.GetLength(0); $r++) {
                    if ($vals[$r, 1] -is [double] -or $vals[$r, 1] -is [int]) {
                        $vals[$r, 1] = $vals[$r, 1] / 1000.0
                    }
                }
                $range.Value2 = $vals
            } else {
                $sheet.Cells.Item(1, $i).Value2 = "IC50 (mg)"
            }
        }
        elseif ($header -match "Zeta_Potential_mV" -or $header -match "Zeta_Potential") {
            $sheet.Cells.Item(1, $i).Value2 = "Zeta Potential (ZP) (mV)"
            Write-Host "Updating Zeta Potential"
        }
        elseif ($header -match "Hydrodynamic_Size" -or $header -match "Size") {
            if ($header -notmatch "Primary_Size") {
                $sheet.Cells.Item(1, $i).Value2 = "Hydrodynamic Size (HS) (nm)"
                Write-Host "Updating Hydrodynamic Size"
            }
        }
        elseif ($header -match "Viability") {
            # Find the next empty column to insert the binary label and 4-tier level
            $binaryCol = $cols + 1
            $tierCol = $cols + 2
            
            $sheet.Cells.Item(1, $binaryCol).Value2 = "Toxicity_Label"
            $sheet.Cells.Item(1, $tierCol).Value2 = "Toxicity_Level"
            Write-Host "Deriving target variables from Cell Viability at index $i"

            # Read all viability values
            $range = $sheet.Range($sheet.Cells.Item(2, $i), $sheet.Cells.Item($sheet.UsedRange.Rows.Count, $i))
            $vals = $range.Value2
            
            # Prepare arrays for new columns
            $binaryVals = New-Object -TypeName 'System.Object[,]' -ArgumentList $vals.GetLength(0), 1
            $tierVals = New-Object -TypeName 'System.Object[,]' -ArgumentList $vals.GetLength(0), 1

            for ($r = 1; $r -le $vals.GetLength(0); $r++) {
                $val = $vals[$r, 1]
                if ($val -is [double] -or $val -is [int]) {
                    # Binary Label: < 60% is toxic (1), >= 60% is non-toxic (0)
                    if ($val -lt 60) {
                        $binaryVals[$r, 1] = 1
                    } else {
                        $binaryVals[$r, 1] = 0
                    }

                    # 4-tier toxicity level
                    if ($val -lt 40) {
                        $tierVals[$r, 1] = "High Toxicity (<40%)"
                    } elseif ($val -lt 60) {
                        $tierVals[$r, 1] = "Significant Toxicity (40-60%)"
                    } elseif ($val -lt 80) {
                        $tierVals[$r, 1] = "Mild to Moderate Toxicity (60-80%)"
                    } else {
                        $tierVals[$r, 1] = "Low/Negligible Toxicity (80-100%)"
                    }
                } else {
                    $binaryVals[$r, 1] = $null
                    $tierVals[$r, 1] = $null
                }
            }
            
            # Write new columns
            $sheet.Range($sheet.Cells.Item(2, $binaryCol), $sheet.Cells.Item($sheet.UsedRange.Rows.Count, $binaryCol)).Value2 = $binaryVals
            $sheet.Range($sheet.Cells.Item(2, $tierCol), $sheet.Cells.Item($sheet.UsedRange.Rows.Count, $tierCol)).Value2 = $tierVals
        }
    }

    $newPath = "C:\Users\admin\OneDrive\Desktop\Data_structure_python\updated sheet.xlsx"
    $workbook.SaveAs($newPath)
    $workbook.Close()
    Write-Host "Saved to updated sheet.xlsx"
} finally {
    $excel.Quit()
    [System.Runtime.Interopservices.Marshal]::ReleaseComObject($excel) | Out-Null
}
Start-Process "C:\Users\admin\OneDrive\Desktop\Data_structure_python\updated sheet.xlsx"
