const axios = require('axios');
const uuid = require('uuid');
const postgres = require('postgres');
const moment = require('moment');
const dotenv = require('dotenv')
const dotenvExpand = require('dotenv-expand');
const myEnv = dotenv.config()
dotenvExpand.expand(myEnv)

const getDetail =(r) => {
  const dataUsaha = r.data_usaha;
  return {
      pt: dataUsaha && dataUsaha.split("<br>")[0],
      modal: dataUsaha && dataUsaha.split("<br>")[1],
      alamat: dataUsaha && dataUsaha.split("<br>")[2],
  }
}

const getLocation = (alamat) => {
  const indexOfKel = alamat && alamat.lastIndexOf("Kelurahan ");
  const indexOfKel2 = alamat && alamat.lastIndexOf(", Kel.");
  const indexOfKec = alamat && alamat.lastIndexOf(", Kec.");
  const indexOfKab = alamat && alamat.indexOf(", Kab ");
  const indexOfKab3 = alamat && alamat.indexOf(", Kab.");
  const indexOfKab2 = alamat && alamat.lastIndexOf(", Kab.");
  const indexOfKota = alamat && alamat.lastIndexOf(", Kota");
  const indexOfProvinsi = alamat && alamat.lastIndexOf(", Provinsi");
  const indexOfProvinsi2 = alamat && alamat.lastIndexOf(", Prov.")
  const indexOfKodePos = alamat && alamat.lastIndexOf(", Kode Pos")
  let kel, kec, kota, provinsi;
  try {
      kel = alamat && alamat.substring(indexOfKel >= 0 ? indexOfKel+ 10 : indexOfKel2 + 6, indexOfKec).trim();
      if (indexOfKel < 0 && indexOfKel2 <0) {
          kel = "";
      }

      kec = alamat && alamat.substring(indexOfKec + 6, whichIndex(indexOfKec, indexOfKab, indexOfKab3, indexOfKab2, indexOfKota)).trim();

      if (indexOfKec < 0) {
          kec = "";
      }

      // console.log(indexOfKab2 >= 0 ? indexOfKab2 + 6 : indexOfKab >= 0 ? indexOfKab + 6: indexOfKota + 6, whichIndex(whichIndex(0, indexOfKab, indexOfKab2, indexOfKota), indexOfProvinsi, indexOfProvinsi2));

      kota = alamat && alamat.substring(indexOfKab2 >= 0 ? indexOfKab2 + 6 : indexOfKab >= 0 ? indexOfKab + 6: indexOfKota + 6, whichIndex(indexOfKab2 >= 0 ? indexOfKab2 + 6 : indexOfKab >= 0 ? indexOfKab + 6: indexOfKota + 6, indexOfProvinsi, indexOfProvinsi2)).trim();
      provinsi = alamat && alamat.substring(indexOfProvinsi + 11, whichIndex(indexOfProvinsi, indexOfKodePos, alamat.length)).trim();
      // console.log(indexOfProvinsi, indexOfProvinsi2, indexOfKodePos);
  } catch(err) {
      console.log(err);
  }

  return {kel, kec, kota, provinsi, alamat}
}

const mapStatus = {
  "00":"pemenuhan",
  "81":"pemenuhan",
  "32": "pemenuhan",
  "40": "pemenuhan",
  "35": "pemenuhan",
  "45": "disetujui",
  "82": "disetujui",
  "33": "disetujui",
  "50": "diterbitkan",
  "51": "diterbitkan",
  "47": "dikembalikan",
  "31": "dikembalikan",
  "34": "dikembalikan",
  "90": "ditolak",
  "93": "ditolak"
}

const whichIndex = (currIndex, ...indexes) => {
  // console.log(currIndex,indexes);
  let min = 0;
  for (const index of indexes) {
    if (index < 0) {
      continue
    }

    if (currIndex > index) {
      continue
    }

    if (min === 0) {
      min = index;
    }

    if (min > index) {
      min = index;
    }
  }
  return min;
}

const transform = (r) => {
  const {pt, modal, alamat} = getDetail(r);
  const {kel, kec, kota, provinsi} = getLocation(alamat);
  const tglPermohonan = r.tgl_pemenuhan_syarat || r.tgl_perubahan || r.tgl_terbit_oss;
  
  return {
      id_permohonan: r.id_permohonan,
      id_proyek: r.id_proyek,
      no_permohonan: r.id_permohonan_izin,
      no_proyek: r.nomor_proyek, 
      tgl_permohonan: tglPermohonan,
      risiko: r.resiko,
      jenis_nib: r.jenis_nib,
      nib: r.nib, 
      kbli: r.kbli, 
      sektor: r.sektor_usaha, 
      jenis_perusahaan: r.jenis_nib, 
      pt: pt, 
      modal: modal, 
      alamat: alamat, 
      kelurahan: kel, kecamatan: kec, kota, provinsi, 
      jenis_proyek: r.jenis_proyek, 
      nama_perizinan: r.nama_dokumen,
      skala: r.skala_usaha,
      status_permohonan: r.status_izin,
      status_id: r.status_respon,
      status_translate: mapStatus[r.status_respon],
      kd_resiko: r.kd_resiko,
      // tgl_terbit_oss: r.tgl_terbit_oss,
      // tgl_pemenuhan_syarat: r.tgl_pemenuhan_syarat,
      // tgl_perubahan:r.tgl_perubahan,
      luas_proyek: r.luas_proyek,
      jenis_proyek: r.jenis_proyek
  }
}

const {OSS_USER,
  OSS_PASSWORD,
  DATABASE_URL,
  OSS_USER_KEY,
  OSS_AUTH_HEADER} = process.env;

const main = async (statuses) => {
    const sql = db();
    const loginUrl = `https://api-prd.oss.go.id/v1/sso/new/users/login?visitorId=${uuid.v4()}`;
    const response = await axios.post(loginUrl, {"username": OSS_USER,"password":OSS_PASSWORD,"pelapor":false,"remember_me":false}, {headers: {user_key: OSS_USER_KEY, "content-type": "application/json", "authorization": OSS_AUTH_HEADER}})
    const {access_token} = response.data.data;
    console.log("Success Logged In With accessToken "+access_token)

    const status =  ["90","93","00","81","32","40","35","45","82","33","50","51","47","31","34"];
    const response2 = await axios.post("https://api-prd.oss.go.id/v1/webform-read/listTableWebform", {"permohonan":{"status":status,"id_profile":"1224794340","kbli":""},"columns":[{"data":"id_permohonan_izin","searchable":false,"sortable":false},{"data":"id_permohonan"},{"data":"id_proyek"},{"data":"nomor_proyek"},{"data":"data_usaha"},{"data":"nama_dokumen"},{"data":"resiko"},{"data":"jenis_nib"},{"data":"status_izin"},{"data":"skala_usaha"},{"data":"status_respon"},{"data":"kd_resiko"},{"data":"kbli"},{"data":"nib"},{"data":"sektor_usaha"},{"data":"id_profile"},{"data":"tgl_terbit_oss"},{"data":"luas_proyek"},{"data":"jenis_proyek"},{"data":"tanggal_terbit"},{"data":"tanggal_berakhir"},{"data":"flag_integrasi"},{"data":"jenis_perizinan"},{"data":"id_lic"},{"data":"flag_terbit_produk_baru"},{"data":"flag_perpanjangan"},{"data":"flag_perubahan"},{"data":"tgl_perubahan"},{"data":"tgl_pemenuhan_syarat"},{"data":"flag_perubahan_data_teknis"}],"order":{"column":"tgl_terbit_oss","dir":"asc"},"page":1,"perPage":10}, {headers: {user_key: OSS_USER_KEY, "content-type": "application/json", "authorization": `Basic ${access_token}`}});
    const {recordsTotal} = response2.data;
    console.log("Success find records Total: " + recordsTotal)
    const response3 = await axios.post("https://api-prd.oss.go.id/v1/webform-read/listTableWebform", {"permohonan":{"status":status,"id_profile":"1224794340","kbli":""},"columns":[{"data":"id_permohonan_izin","searchable":false,"sortable":false},{"data":"id_permohonan"},{"data":"id_proyek"},{"data":"nomor_proyek"},{"data":"data_usaha"},{"data":"nama_dokumen"},{"data":"resiko"},{"data":"jenis_nib"},{"data":"status_izin"},{"data":"skala_usaha"},{"data":"status_respon"},{"data":"kd_resiko"},{"data":"kbli"},{"data":"nib"},{"data":"sektor_usaha"},{"data":"id_profile"},{"data":"tgl_terbit_oss"},{"data":"luas_proyek"},{"data":"jenis_proyek"},{"data":"tanggal_terbit"},{"data":"tanggal_berakhir"},{"data":"flag_integrasi"},{"data":"jenis_perizinan"},{"data":"id_lic"},{"data":"flag_terbit_produk_baru"},{"data":"flag_perpanjangan"},{"data":"flag_perubahan"},{"data":"tgl_perubahan"},{"data":"tgl_pemenuhan_syarat"},{"data":"flag_perubahan_data_teknis"}],"order":{"column":"tgl_terbit_oss","dir":"asc"},"page":1,"perPage":recordsTotal}, {headers: {user_key: OSS_USER_KEY, "content-type": "application/json", "authorization": `Basic ${access_token}`}});
    const formatResponse = response3.data.items.map(transform);
    
    // console.log("mapping responses")
    // const formatResponse = require('./oss-res.json').items.map(transform);

    const mappedResponse = formatResponse.map(r => {
      const alamat = r.alamat;
      delete r.alamat;
      r.json = JSON.stringify(r); 
      r.created_at = moment().format("YYYY-MM-DD HH:mm:ss");
      r.updated_at = moment().format("YYYY-MM-DD HH:mm:ss");
      return {alamat, ...r};
    });
    
    const columns = ['no_permohonan', 'json']

    console.log("querying rekap_oss..")
    const rekap_oss = await sql`select ${ sql(columns) } from rekap_oss`;

    const update = [];
    const insertNew = [];
    const mapIdNew = {};

    console.log("separate insert and update")
    for (let map of mappedResponse) {
        const no_permohonans = rekap_oss.map(r => r.no_permohonan);
        if (no_permohonans.includes(map.no_permohonan)) {
            const json = rekap_oss.filter(r => r.no_permohonan === map.no_permohonan)[0].json;
            if (map.json != json) {
              if (checkVarDiff(map.json, json)) {
                update.push(map);
              }
            }
        } else {
            if (!mapIdNew[map.no_permohonan]) {
              insertNew.push(map);
              mapIdNew[map.no_permohonan] = true;
            }
        }
    }

    console.log(`update ${update.length}, insert ${insertNew.length}`)

    if (update.length === 0 && insertNew.length === 0) {
      console.log("no new insert and update");
      return;
    }

    if (update.length > 0) {
        for (let up of update) { 
          try {
            console.log(`updating... ${up.no_permohonan}`);
            const rekap_oss = await sql`select ${ sql(['id_permohonan','id_proyek','no_permohonan','no_proyek','tgl_permohonan','risiko','jenis_nib','nib','kbli','sektor','jenis_perusahaan','pt','modal','alamat','kelurahan','kecamatan','kota','provinsi','jenis_proyek','nama_perizinan','skala','status_permohonan','status_id','status_translate','kd_resiko','luas_proyek', 'json', 'created_at']) } from rekap_oss where no_permohonan = ${up.no_permohonan}`;
            const data = rekap_oss[0];
            console.log(`inserting to rekap_oss_history... ${up.no_permohonan}`);
            await sql`insert into rekap_oss_history ${ sql([data], 'id_permohonan','id_proyek','no_permohonan','no_proyek','tgl_permohonan','risiko','jenis_nib','nib','kbli','sektor','jenis_perusahaan','pt','modal','alamat','kelurahan','kecamatan','kota','provinsi','jenis_proyek','nama_perizinan','skala','status_permohonan','status_id','status_translate','kd_resiko','luas_proyek', 'json', 'created_at') }`
            
            // DELETE AND INSERT UPLOAD
            console.log(`deleting upload ${up.no_permohonan}`)
            await sql`delete from rekap_oss_upload where no_permohonan = ${up.no_permohonan}`     
            const url = `https://api-prd.oss.go.id/v1/webform/main_proses/getFormData?id_permohonan=${up.id_permohonan}&id_proyek=${up.id_proyek}&isProyekSyarat=true&isIzin=true&isProyekLokasiLaut=true&isNotifikasiLaut=true&isIMBSLF=true&isNotifikasiHutan=true&id_permohonan_izin=${up.no_permohonan}&isPerpanjangan=true`
            const res = await axios({
                method: "GET",
                url,
                headers: {user_key: "846ee507525c6b00d18733e066bd5686"}});
            const insertUpload = res.data.dataProyekSyarat.map(n => ({no_permohonan: up.no_permohonan, id_upload: n.id_upload || "", id_syarat: n.id_izin_syarat}));
            console.log(`insert upload ${up.no_permohonan} ${insertUpload.map(r => r.id_syarat + " " + r.id_upload).join(",")}`)
            if (insertUpload.length > 0) {
                console.log("inserting into rekap_oss_upload")
                await sql`insert into rekap_oss_upload ${ sql(insertUpload, 'no_permohonan','id_upload', 'id_syarat') }`;
            }
            
            await sql` update rekap_oss set ${sql(up, 'id_permohonan','id_proyek','no_proyek','tgl_permohonan','risiko','jenis_nib','nib','kbli','sektor','jenis_perusahaan','pt','modal','alamat','kelurahan','kecamatan','kota','provinsi','jenis_proyek','nama_perizinan','skala','status_permohonan','status_id','status_translate','kd_resiko','luas_proyek', 'json', 'updated_at')} 
              where no_permohonan = ${ up.no_permohonan }`
          } catch (err) {
            console.log(err);
          }
        }
    };

    
    if (insertNew.length > 0) {
      console.log("inserting rows " + insertNew.length);
      var size = 500; var arrayOfArrays = [];
      for (var i=0; i<insertNew.length; i+=size) {
          arrayOfArrays.push(insertNew.slice(i,i+size));
      }

      for (let array of arrayOfArrays) {
        await sql`insert into rekap_oss ${ sql(array, 'id_permohonan','id_proyek','no_permohonan','no_proyek','tgl_permohonan','risiko','jenis_nib','nib','kbli','sektor','jenis_perusahaan','pt','modal','alamat','kelurahan','kecamatan','kota','provinsi','jenis_proyek','nama_perizinan','skala','status_permohonan','status_id','status_translate','kd_resiko','luas_proyek', 'json', 'created_at') }`
        for (let up of array) {
          console.log(`deleting upload ${up.no_permohonan}`)
          await sql`delete from rekap_oss_upload where no_permohonan = ${up.no_permohonan}`     
          const url = `https://api-prd.oss.go.id/v1/webform/main_proses/getFormData?id_permohonan=${up.id_permohonan}&id_proyek=${up.id_proyek}&isProyekSyarat=true&isIzin=true&isProyekLokasiLaut=true&isNotifikasiLaut=true&isIMBSLF=true&isNotifikasiHutan=true&id_permohonan_izin=${up.no_permohonan}&isPerpanjangan=true`
          const res = await axios({
              method: "GET",
              url,
              headers: {user_key: "846ee507525c6b00d18733e066bd5686"}});
          const insertUpload = res.data.dataProyekSyarat.map(n => ({no_permohonan: up.no_permohonan, id_upload: n.id_upload || "", id_syarat: n.id_izin_syarat}));
          console.log(`insert upload ${up.no_permohonan} ${insertUpload.map(r => r.id_syarat + " " + r.id_upload).join(",")}`)
          if (insertUpload.length > 0) {
              console.log("inserting into rekap_oss_upload")
              await sql`insert into rekap_oss_upload ${ sql(insertUpload, 'no_permohonan','id_upload', 'id_syarat') }`;
          }
          
          await sql` update rekap_oss set ${sql(up, 'id_permohonan','id_proyek','no_proyek','tgl_permohonan','risiko','jenis_nib','nib','kbli','sektor','jenis_perusahaan','pt','modal','alamat','kelurahan','kecamatan','kota','provinsi','jenis_proyek','nama_perizinan','skala','status_permohonan','status_id','status_translate','kd_resiko','luas_proyek', 'json', 'updated_at')} 
            where no_permohonan = ${ up.no_permohonan }`
        }
      }
    }
}

const checkVarDiff = (json, json2) => {
  json = JSON.parse(json);
  json2 = JSON.parse(json2);
  const beda = [];
  for (let keys of Object.keys(json)) {
    if (json[keys] !== json2[keys]) {
      beda.push(keys);
    }
  }

  if (beda.includes("alamat") || beda.includes("kelurahan") || beda.includes("upload")) {
    return false;
  } else {
    console.log(`perbedaan ${json.no_permohonan} ada di ${beda}`)
    return true;
  }
}

const db = () => {
    const sql = postgres(DATABASE_URL);
    return sql;
}

main().then(console.log).catch(console.log).finally(() => process.exit())