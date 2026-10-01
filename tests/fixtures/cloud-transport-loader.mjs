import path from'node:path';import{pathToFileURL}from'node:url';
const{resolve:stagingResolve}=await import(pathToFileURL(path.join(process.env.CENTRIX_RC_DIR,'tests/fixtures/whatsapp-staging-loader.mjs')));
export async function resolve(specifier,context,next){
 if(specifier==='baileys'&&process.env.CENTRIX_BAILEYS_MOCK==='true')return{url:new URL('./cloud-baileys-mock.mjs',import.meta.url).href,shortCircuit:true};
 return stagingResolve(specifier,context,next);
}
