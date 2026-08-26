package store

import (
	"context"
	"fmt"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf/store/mtfquery"
)

func (d *Database) ListMediaFamilies(ctx context.Context) ([]MediaFamily, error) {
	rows, err := d.readQueries.ListMediaFamilies(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]MediaFamily, 0, len(rows))
	for _, r := range rows {
		out = append(out, d.enrichFamily(ctx, familyFromRow(r)))
	}
	return out, nil
}

func (d *Database) GetMediaFamily(ctx context.Context, id int64) (MediaFamily, error) {
	r, err := d.readQueries.GetMediaFamily(ctx, id)
	if err != nil {
		return MediaFamily{}, mapErr(err, "media family")
	}
	return d.enrichFamily(ctx, familyFromRow(r)), nil
}

func (d *Database) enrichFamily(ctx context.Context, f MediaFamily) MediaFamily {
	carts, err := d.readQueries.ListCartridgesByFamily(ctx, f.ID)
	if err != nil {
		log.Error(err, "")
	}
	f.CartridgeCount = len(carts)
	dsets, err := d.readQueries.ListDataSetsByFamily(ctx, f.ID)
	if err != nil {
		log.Error(err, "")
	}
	f.DataSetCount = len(dsets)
	if f.Name == "" {
		f.Name = fmt.Sprintf("Media-Family-%d", f.ID)
	}
	return f
}

func familyFromRow(r mtfquery.MediaFamily) MediaFamily {
	return MediaFamily{
		ID:          r.ID,
		Name:        ns(r.Name),
		TotalTapes:  ni(r.TotalTapes),
		HasCatalog:  nb(r.HasCatalog),
		LastScanned: ni64(r.LastScanned),
		CreatedAt:   ni64(r.CreatedAt),
	}
}

func (d *Database) ListCartridges(ctx context.Context) ([]Cartridge, error) {
	rows, err := d.readQueries.ListCartridges(ctx)
	if err != nil {
		return nil, err
	}
	famNames := d.familyNameCache(ctx)
	out := make([]Cartridge, 0, len(rows))
	for _, r := range rows {
		c := cartridgeFromRow(r)
		c.MediaFamilyName = famNames[c.MediaFamilyID]
		out = append(out, c)
	}
	return out, nil
}

func (d *Database) ListCartridgesByFamily(ctx context.Context, familyID int64) ([]Cartridge, error) {
	rows, err := d.readQueries.ListCartridgesByFamily(ctx, familyID)
	if err != nil {
		return nil, err
	}
	out := make([]Cartridge, 0, len(rows))
	for _, r := range rows {
		out = append(out, cartridgeFromRow(r))
	}
	return out, nil
}

func (d *Database) GetCartridge(ctx context.Context, barcode string) (Cartridge, error) {
	r, err := d.readQueries.GetCartridge(ctx, barcode)
	if err != nil {
		return Cartridge{}, mapErr(err, "cartridge")
	}
	return cartridgeFromRow(r), nil
}

func (d *Database) DeleteCartridge(ctx context.Context, barcode string) error {
	_, err := d.queries.DeleteCartridge(ctx, barcode)
	return err
}

func cartridgeFromRow(r mtfquery.MtfCartridge) Cartridge {
	return Cartridge{
		Barcode:         r.Barcode,
		Label:           ns(r.Label),
		MediaFamilyID:   r.MediaFamilyID,
		Sequence:        ni(r.Sequence),
		Role:            ns(r.Role),
		CatalogType:     ni(r.CatalogType),
		IsBkfFile:       nb(r.IsBkfFile),
		SourcePath:      ns(r.SourcePath),
		Volumes:         ni(r.Volumes),
		Directories:     ni(r.Directories),
		Files:           ni(r.Files),
		EmptyFiles:      ni(r.EmptyFiles),
		FileBytes:       ni64(r.FileBytes),
		SparseFiles:     ni(r.SparseFiles),
		CompressedFiles: ni(r.CompressedFiles),
		EncryptedFiles:  ni(r.EncryptedFiles),
		HasCatalog:      nb(r.HasCatalog),
		CatalogBytes:    ni64(r.CatalogBytes),
		SetsClosed:      ni(r.SetsClosed),
		Status:          ns(r.Status),
		LastScanned:     ni64(r.LastScanned),
		CreatedAt:       ni64(r.CreatedAt),
	}
}

func (d *Database) ListAllDataSets(ctx context.Context) ([]DataSet, error) {
	rows, err := d.readQueries.ListAllDataSets(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]DataSet, 0, len(rows))
	for _, r := range rows {
		out = append(out, dataSetFromRow(r))
	}
	return out, nil
}

func (d *Database) ListDataSetsByFamily(ctx context.Context, familyID int64) ([]DataSet, error) {
	rows, err := d.readQueries.ListDataSetsByFamily(ctx, familyID)
	if err != nil {
		return nil, err
	}
	out := make([]DataSet, 0, len(rows))
	for _, r := range rows {
		ds := dataSetFromRow(r)
		vols, err := d.readQueries.ListVolumesByDataSet(ctx, r.ID)
		if err != nil {
			log.Error(err, "")
		}
		ds.Volumes = volumesFromRows(vols)
		tapes, err := d.readQueries.ListDataSetTapes(ctx, r.ID)
		if err != nil {
			log.Error(err, "")
		}
		ds.Tapes = dataSetTapesFromRows(tapes)
		out = append(out, ds)
	}
	return out, nil
}

func (d *Database) GetDataSet(ctx context.Context, id int64) (DataSet, error) {
	r, err := d.readQueries.GetDataSet(ctx, id)
	if err != nil {
		return DataSet{}, mapErr(err, "data set")
	}
	ds := dataSetFromRow(r)
	vols, err := d.readQueries.ListVolumesByDataSet(ctx, r.ID)
	if err != nil {
		log.Error(err, "")
	}
	ds.Volumes = volumesFromRows(vols)
	tapes, err := d.readQueries.ListDataSetTapes(ctx, r.ID)
	if err != nil {
		log.Error(err, "")
	}
	ds.Tapes = dataSetTapesFromRows(tapes)
	return ds, nil
}

func (d *Database) SetDataSetSsetPba(ctx context.Context, id int64, pba int64) error {
	_, err := d.queries.UpdateDataSetSsetPba(ctx, mtfquery.UpdateDataSetSsetPbaParams{
		SsetPba: pba,
		ID:      id,
	})
	return err
}

func dataSetFromRow(r mtfquery.DataSet) DataSet {
	return DataSet{
		ID:             r.ID,
		MediaFamilyID:  r.MediaFamilyID,
		SetNumber:      ni(r.SetNumber),
		Name:           ns(r.Name),
		Description:    ns(r.Description),
		Owner:          ns(r.Owner),
		MachineName:    ns(r.MachineName),
		WriteTime:      ni64(r.WriteTime),
		NumDirectories: ni(r.NumDirectories),
		NumFiles:       ni(r.NumFiles),
		NumCorrupt:     ni(r.NumCorrupt),
		Size:           ni64(r.Size),
		SSETPBA:        r.SsetPba,
		FirstMediaSeq:  ni(r.FirstMediaSeq),
		SourceMediaSeq: ni(r.SourceMediaSeq),
	}
}

func volumesFromRows(rows []mtfquery.DataSetVolume) []DataSetVolume {
	out := make([]DataSetVolume, 0, len(rows))
	for _, r := range rows {
		out = append(out, DataSetVolume{
			ID:              r.ID,
			DataSetID:       r.DataSetID,
			Device:          ns(r.Device),
			VolumeLabel:     ns(r.VolumeLabel),
			MachineName:     ns(r.MachineName),
			MappedNamespace: ns(r.MappedNamespace),
		})
	}
	return out
}

func dataSetTapesFromRows(rows []mtfquery.DataSetTape) []DataSetTape {
	out := make([]DataSetTape, 0, len(rows))
	for _, r := range rows {
		out = append(out, DataSetTape{
			ID:        r.ID,
			DataSetID: r.DataSetID,
			MediaSeq:  r.MediaSeq,
			SSETPBA:   r.SsetPba,
		})
	}
	return out
}

func (d *Database) familyNameCache(ctx context.Context) map[int64]string {
	rows, err := d.readQueries.ListMediaFamilies(ctx)
	if err != nil {
		return nil
	}
	m := make(map[int64]string, len(rows))
	for _, r := range rows {
		name := ns(r.Name)
		if name == "" {
			name = fmt.Sprintf("Media-Family-%d", r.ID)
		}
		m[r.ID] = name
	}
	return m
}
