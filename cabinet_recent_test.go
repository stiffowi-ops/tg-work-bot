package main
import("testing";"path/filepath")
func TestRecentMaterialsPersistence(t *testing.T){
 path:=filepath.Join(t.TempDir(),"employees.json")
 s,e:=openStore(path);if e!=nil{t.Fatal(e)}
 if _,e=s.put(employee{ID:"1",Name:"One"},true);e!=nil{t.Fatal(e)}
 if _,e=s.put(employee{ID:"2",Name:"Two"},true);e!=nil{t.Fatal(e)}
 for _,ref:=range []string{"calendar:a","industry:b","calendar:a"}{if _,e=s.recordRecent("1",ref);e!=nil{t.Fatal(e)}}
 if _,e=s.put(employee{ID:"1",Name:"Changed"},false);e!=nil{t.Fatal(e)}
 s,e=openStore(path);if e!=nil{t.Fatal(e)}
 p,_:=s.get("1");other,_:=s.get("2")
 if len(p.RecentMaterials)!=2||p.RecentMaterials[0]!="calendar:a"||len(other.RecentMaterials)!=0{t.Fatal(p,other)}
}
