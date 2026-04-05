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
